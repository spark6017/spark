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
   * 平衡因子0.7
   */
  private val LOAD_FACTOR = 0.7


  /** *
    * 对初始容量进行标准化，将initialCapacity转化为比它大的最小的2次幂数值
    *
    * capacity实际上记录的是元素个数容量
    */
  private var capacity = nextPowerOf2(initialCapacity)


  /** *
    * mask是干啥的？Key做Hash后和mask做与运算，保证pos在0和capacity之间
    */
  private var mask = capacity - 1

  /** *
    * 当前AppendOnlyMap中的数据容量
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
   * 也就是说capacity是元素个数，每个元素有K和V，因此data这个数组的大小是2*capacity
   */
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  /**表示AppendOnlyMap包含有值为Null的Key**/
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
   * */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]

    /**
     * 如果Key是null，则返回null key对应的值，nullValue
     */
    if (k.eq(null)) {
      return nullValue
    }

    //获得元素的位置，为什么要rehash加mask，mask是保证pos在0-capacity之间，否则pos就越界了
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) { /**while循环，何时结束？**/
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else { /**解决碰撞，怎么解决的？*/
        val delta = i  /**解决碰撞过程：pos是累加delta**/
        pos = (pos + delta) & mask
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
      *
      */

    ///代码到不了这里！前面已经是死循环了
    null.asInstanceOf[V]
  }

  /** Set the value for a key */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)

      /** *
        * 如果pos*2这个位置没有放置KV，那么就是一个新增元素，添加到当前位置，并且size增1
        *
        * Key存放的位置永远是偶数位置？
        */
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) { /**如果该位置已经有了元素，首先判断该元素是否就是要放入的元素，如果是，则表示update操作，修改data(2*pos+1)**/
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        val delta = i  /**delta赋值为1，获取新的position**/
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else {
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
   * AppendOnlyMap返回一个Iterator
    */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {

      //pos是Iterator内部类的成员变量
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
       * 是否还有元素
       * @return
       */
      override def hasNext: Boolean = nextValue() != null

      /**
       * 获取下一个元素
       * @return
       */
      override def next(): (K, V) = {
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
   *  Hash扩容，容量增加1倍
   *
   * */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
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
    data = newData
    capacity = newCapacity
    mask = newMask
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
   * 返回AppendOnlyMap中排序后的<K,V> pairs
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))


    /**
     * 在data数组上执行排序操作(基于TimSort进行排序)
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
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
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
