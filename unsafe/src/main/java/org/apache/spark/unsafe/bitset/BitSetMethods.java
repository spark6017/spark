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

package org.apache.spark.unsafe.bitset;

import org.apache.spark.unsafe.Platform;

/**
 * Methods for working with fixed-size uncompressed bitsets.
 *
 * We assume that the bitset data is word-aligned (that is, a multiple of 8 bytes in length).
 *
 * Each bit occupies exactly one bit of storage.
 */
public final class BitSetMethods {

  /***
   * BitSet是8的整数倍个字节
   */
  private static final long WORD_SIZE = 8;

  private BitSetMethods() {
    // Make the default constructor private, since this only holds static methods.
  }

  /**
   * Sets the bit at the specified index to {@code true}.
   *
   * 为index列对应的null tracking bitset设置值。这里仅仅是设置bitset的值，
   *
   * 算法：
   * 1. 求出存放数据的offset
   * 2. 计算要存入什么值
   */
  public static void set(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";


    /***
     *  index是整数类型，index&0x3f表示对64取模，index&0x3f的取值区间是[0,63]
     *  如果index<=63,那么index&0x3f就等于index;如果index>63，那么index&0x3f表示index%64
     *
     *
     * 如果index<=63,那么mask的值是1向左移动index个位置。也就是说，mask=2^index
     * mask的最大值是1<<63,这是整数表示的最大范围。index有很多值使得index&0x3f=63
     */
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift

    /***
     *  index向右移动6位，如果index=64，即2^6,那么index>>64=1
     *  如果index [0,63),那么index>>6=0,
     *  这表示wordOffset的值
     */
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;

    //取出原始值
    final long word = Platform.getLong(baseObject, wordOffset);

    //将mask值写入
    Platform.putLong(baseObject, wordOffset, word | mask);
  }

  /**
   * Sets the bit at the specified index to {@code false}.
   */
  public static void unset(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;
    final long word = Platform.getLong(baseObject, wordOffset);

    //set的操作是word|mask, unset的操作是wor&~mask
    Platform.putLong(baseObject, wordOffset, word & ~mask);
  }

  /**
   * Returns {@code true} if the bit is set at the specified index.
   *
   * 判断index列是否为null
   */
  public static boolean isSet(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";

    /**mask值*/
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;

    final long word = Platform.getLong(baseObject, wordOffset);

    //如果设置了，那么word的值是word | mask,
    //(word | mask) & mask是否一定不为0？
    //(word & mask)的结果应该是mask
    return (word & mask) != 0;
  }

  /**
   * Returns {@code true} if any bit is set.
   *
   * UnsafeRow中是否有值为NULL的列
   *
   * @param baseObject
   * @param baseOffset
   * @param bitSetWidthInWords bitsetWidthInWords,它是UnsafeRow#calculateBitSetWidthInBytes/8
   * @return
   */
  public static boolean anySet(Object baseObject, long baseOffset, long bitSetWidthInWords) {
    //add是baseOffset,offset的偏移量是WORD_SIZE为单位
    long addr = baseOffset;

    //一共循环bitSetWidthInWords次
    for (int i = 0; i < bitSetWidthInWords; i++, addr += WORD_SIZE) {
      if (Platform.getLong(baseObject, addr) != 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the index of the first bit that is set to true that occurs on or after the
   * specified starting index. If no such bit exists then {@code -1} is returned.
   * <p>
   * To iterate over the true bits in a BitSet, use the following loop:
   * <pre>
   * <code>
   *  for (long i = bs.nextSetBit(0, sizeInWords); i &gt;= 0; i = bs.nextSetBit(i + 1, sizeInWords)) {
   *    // operate on index i here
   *  }
   * </code>
   * </pre>
   *
   * @param fromIndex the index to start checking from (inclusive)
   * @param bitsetSizeInWords the size of the bitset, measured in 8-byte words
   * @return the index of the next set bit, or -1 if there is no such bit
   */
  public static int nextSetBit(
      Object baseObject,
      long baseOffset,
      int fromIndex,
      int bitsetSizeInWords) {
    int wi = fromIndex >> 6;
    if (wi >= bitsetSizeInWords) {
      return -1;
    }

    // Try to find the next set bit in the current word
    final int subIndex = fromIndex & 0x3f;
    long word = Platform.getLong(baseObject, baseOffset + wi * WORD_SIZE) >> subIndex;
    if (word != 0) {
      return (wi << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word);
    }

    // Find the next set bit in the rest of the words
    wi += 1;
    while (wi < bitsetSizeInWords) {
      word = Platform.getLong(baseObject, baseOffset + wi * WORD_SIZE);
      if (word != 0) {
        return (wi << 6) + java.lang.Long.numberOfTrailingZeros(word);
      }
      wi += 1;
    }

    return -1;
  }
}
