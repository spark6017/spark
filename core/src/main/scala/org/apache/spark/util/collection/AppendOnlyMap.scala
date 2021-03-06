/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection

import java.util.{Arrays, Comparator}

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing (二次探测)with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements. (0.7是阀值，capacity的总大小为什么是2^ 29?)
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 *
 * HashMap 是 Spark shuffle read 过程中频繁使用的、用于 aggregate 的数据结构，AppendOnlyMap是全内存的数据结构
 * AppendOnlyMap继承自Iterable[(K，V)]
 *
 *
 * 说明：Spark代码中没有地方直接使用AppendOnlyMap， 不过SizeTrackingAppendOnlyMap继承自AppendOnlyMap
 * 而PartitionedAppendOnlyMap继承自SizeTrackingAppendOnlyMap
 *
 * AppendOnlyMap的应用也不能说不广泛，因为ExternalAppendOnlyMap的底层数据结构是SizeTrackingAppendOnlyMap，而SizeTrackingAppendOnlyMap
 * 就是一个AppendOnlyMap
 *
 * ExternalAppendOnlyMap应用的更为广泛，Shuffle Read基本上都改为使用ExternalAppendOnlyMap了，比如[[org.apache.spark.Aggregator]]
  *
  *
  * AppendOnlyMap是一个继承自Iteratable的集合
  *
 *
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  /** *
    * 最大元素个数，2的29次方，
    */
  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  /**
   * 负载因子0.7，元素个数到达容量的70%时，开始扩容
   */
  private val LOAD_FACTOR = 0.7


  /** *
    * 对初始容量进行标准化，将capacity设置为比initialCapacity大的最小的2次幂数值
    *
    * capacity记录的是元素个数容量,AppendOnlyMap实际占用的空间是2*capacity
    */
  private var capacity = nextPowerOf2(initialCapacity)


  /** *
    * mask是干啥的？Key做Hash后和mask做与运算，保证pos在0和capacity之间
    */
  private var mask = capacity - 1

  /** *
    * 当前AppendOnlyMap中的元素个数
    */
  private var curSize = 0

  /** *
    * 需要扩容的阀值
    */
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.

  /**
   * Key和Value并排放，有利于内存本地性，这里数组大小为什么是2 * capacity？
   *
   * 因为：capacity是元素个数，每个元素有K和V，因此data这个数组的大小是2*capacity。
    *
    * 也就是说，AppendOnlyMap的长度实际上是capacity*2
    *
    * data需要存放Key和Value，所以它是AnyRef类型
   */
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.


  /**表示AppendOnlyMap是否包含有值为Null的Key**/
  private var haveNullValue = false

  /**
   * nullValue是个var，会对它进行赋值？是的，表示Key为null对应的值
   */
  private var nullValue: V = null.asInstanceOf[V]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  private var destroyed = false
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /**
   * Get the value for a given key
   *
   *  根据指定的Key,获得指定的值，从这里Get的过程可以看到Put的过程
    *
    *  appy是一个Get操作，应该跟Put操作是匹配的
   * */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]

    /**
     * 如果Key是null，则返回null key对应的值nullValue
     */
    if (k.eq(null)) {
      return nullValue
    }

    //获得元素的位置，为什么要rehash加mask，mask是保证pos在0-capacity之间，否则pos就越界了
    //取Key的hash值
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) { /**while循环，何时结束？**/
      val curKey = data(2 * pos) //数据Key位于2*pos处
      if (k.eq(curKey) || k.equals(curKey)) { //如果curKey的值跟参数key相等，那么直接返回该key的下一个位置的值(data(2*pos+1)
        return data(2 * pos + 1).asInstanceOf[V] //数据Value位于2*pos + 1处
      } else if (curKey.eq(null)) { //data(2*pos)出的值为null，表示AppendOnlyMap中不包含指定的Key，因此返回的value为null
        return null.asInstanceOf[V]
      } else { /**解决碰撞，怎么解决的？*/
        val delta = i  /**解决碰撞过程：pos是累加delta**/
        pos = (pos + delta) & mask //pos移动delta
        i += 1
      }
    }


    /** **
      *
      * 假设一开始pos为0
      *
      * 第一次碰撞：
      *           delta=1， pos = pos +1 = 1
      *
      *   第二次碰撞
      *           delta = 2， pos = pos +2=3
      *
      *    第三次碰撞
      *           delta = 3   pos = pos + 3=6
      *
      *   第四次碰撞
      *           delta =4 pos = pos + 4 = 10
      *
      *
      *    ...
      *
      *    第N次碰撞， 就在前面pos的基础上加N，步长线性增加
      *
      *
      *    通过上面的碰撞的解决，可以看出，步长是依次增加的
      *
      *
      *
      */

    ///代码到不了这里！前面已经是死循环了
    null.asInstanceOf[V]
  }

  /**
    * Set the value for a key
    * update if exists, put if not exists
    *
    * update方法的真实含义是saveOrUpdate，如果存在则更新；如果不存在则插入
    *
    * @param key
    * @param value
    */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]

    //如果插入的是空值，那么首先判断是否有空值，如果还没有那么map的的长度首先加1，然后更新nullValue的值以及更新haveNullValue的值
    if (k.eq(null)) {
      if (!haveNullValue) { //如果AppendOnlyMap中还没有Key为null的键值对，为什么要调用incrementSize？首先元素个数需要增1
        incrementSize()
      }
      nullValue = value //设置nullValue是value，并且将hasNullValue设置为true
      haveNullValue = true
      return
    }

    /***
      * 计算key对应的存放位置
      */
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)

      /** *
        * 如果pos*2这个位置为空，表示这是一个put操作，那么将KV添加到当前位置，并且size增1
        *
        * Key存放的位置永远是偶数位置？是的
        */
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) { /**如果该位置已经有了元素， 那么判断该位置的元素值是否跟Key相等，如果是则表示update操作，修改data(2*pos+1)**/
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {  /**该位置有数据，但是它的值跟key不同，因此发生了碰撞了，需要查找新的位置**/
        val delta = i  /**delta赋值为1，获取新的position**/
        pos = (pos + delta) & mask  //pos + 1, pos + 3, pos + 6
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
    *
    * 对key进行更新操作，更新逻辑由updateFunc指定,也就是说，不同于update方法，直接给定要更新的值
    * updateFunc的类型是(Boolean, V) => V): V
    *
    * @param key
    * @param updateFunc
    * @return
    */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    /** *
      * 确保AppendOnlyMap没被破坏
      */
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]

    //如果更新的key是null，第一个参数表示该key是否已经有值，第二个参数表示原始值(原来null key对应的null value)
    if (k.eq(null)) {

      //如果目前还没有Null值，那么需要增加一个k为null的entry
      if (!haveNullValue) {
        incrementSize()
      }

      /** *
        * 调用updateFunc方法，传入Key是否已经存在(haveNullValue)和旧的值(nullValue)
        */
      nullValue = updateFunc(haveNullValue, nullValue) /**计算nullValue**/
      haveNullValue = true
      return nullValue
    }

    /***
      * 求Hash值
      */
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        //已经有值，旧值为data(2*pos+1)
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {
        //没有值，旧值为null
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else {
        //解决冲突
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /**
    *
    * Iterator method from Iterable
   *
   *  AppendOnlyMap返回一个Iterator， 遍历数据
    *
    *  AppendOnlyMap继承自Iterable，因此需要实现iterator方法
    *
    * @return
    */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {

      //pos是Iterator内部类的成员变量，-1表示null key的位置
      var pos = -1

      /**
       * Get the next value we should return from next(), or null if we're finished iterating
       *
       * 获取下一个值，
       */
      def nextValue(): (K, V) = {

        /** *
          * 为什么pos是-1表示寻找null值？意思是说，pos为-1的位置记录了Null Key
          */
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }

        /** *
          * 遍历到capacity，这是数组遍历，取第一个不为null的数据，同时记录已经遍历到的位置
          * 读取pos*2位置的元素K和V
          */
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      /**
       * 是否还有元素，调用nextValue的结果是pos位置指向了下一个要遍历的数据
       * @return
       */
      override def hasNext: Boolean = nextValue() != null

      /**
       * 获取下一个元素，pos向前移动，此后调用hasNext时，会计算pos位置以及之后的位置，直到找到一个不为空的元素位置
       * @return
       */
      override def next(): (K, V) = {
        //在hasNext方法已经调用了一遍nextValue，为什么next方法还要再调用一遍nextValue?
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   * 对Hash值进行二次Hash
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /**
   *
   * Double the table's size and re-hash everything
   *  Hash扩容，容量增加1倍，原来的数据需要重新放置，原来的数据需要遍历出来的，此处不是调用hasNext和next方法，而是直接操作数组data
    *  不需要关心null key，因为null key不属于data管理
    *
   *
   * */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow

    //newCapacity的上线是2 ^ 29 * 2 = 2^ 30,而整数的最大值是2 ^ 31 - 1?
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")

    /**
      又创建了一个长度为2 * newCapacity的数组
      */

    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0

    /***
      * 线性遍历capacity次
      */
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)

        /***
          * 计算Key的心为之
          */
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true

        /**
          * 解决碰撞
          */
        while (keepGoing) {
          /**
            * 找到 2*newPos位置
            */
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    data = newData /**原来的data会被GC销毁*/
    capacity = newCapacity
    mask = newMask
    /**重新计算growThreshold*/
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  /**
   * 如果n不是2次幂，那么需要将它转换
   * @param n
   * @return
   */
  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
    *
    * 在原来集合的基础上创建一个排序的集合
   *
   * 返回AppendOnlyMap中排序后的<K,V> pairs
    *
    * @param keyComparator
    * @return
    */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    var keyIndex, newIndex = 0

    /***
      * 将data中的数据移动到数组顶端，数组中存放的KV的位置是通过对K进行哈希算法算出来的，因此可能有很多空巢位置
      * 移动后，data中数据还是按照K，V并排放的，如何进行数组排序？
      * 答案：Tim排序需要指定排序数据的格式，这里指定为KVArraySortDataFormat
      *
      *
      */
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        //newIndex表示实际元素个数计数器
        newIndex += 1
      }
      //keyIndex是遍历数组的循环变量
      keyIndex += 1
    }

    /**newIndex跟curSize最多只差1，newIndex记录了元素个数*/
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))


    /**
     * 在data数组上执行排序操作(基于TimSort进行排序)，排序的
      * KVArraySortDataFormat的Buffer是Array[AnyRef]
     * 为了实现数组排序，因此前面将所有的元素移动到了数组的顶端
      *
     */
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    /***
      * 返回Iterator的内部类对象
      */
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V]) /**data的偶数位置是类型K，需要强制转换；data的奇数位置是类型V，需要强制转换**/
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   *
   *
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29)
}
