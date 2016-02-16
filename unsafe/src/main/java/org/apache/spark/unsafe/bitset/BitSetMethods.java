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
   * 在BitSet，需要找到
   */
  public static void set(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";

    /**index & 0x3f结果是index对64取模？不是的，因为64&64的结果不是0，而是64*/
    /**1<<1的结果是2,1<<2结果是4，即1<<n的结果是2^n*/

    /**新值**/
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift

    /**1>>6是0， 2>>6是0*/
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;

      /***
       * 取出wordOffset处的8个字节构成的long数值
       * 旧值
       */
    final long word = Platform.getLong(baseObject, wordOffset);


    /***
     * 在wordOffset写入一个Long类型的值，也就是在wordOffset之后占用8个字节
     * 这个值是word（旧值）和mask(新值)进行或运算的结果
     */
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
    Platform.putLong(baseObject, wordOffset, word & ~mask);
  }

  /**
   * Returns {@code true} if the bit is set at the specified index.
   * 与set配对操作
   */
  public static boolean isSet(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";

    /**mask值*/
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;

    /**旧值*/
    final long word = Platform.getLong(baseObject, wordOffset);
    return (word & mask) != 0;
  }

  /**
   * Returns {@code true} if any bit is set.
   */
  public static boolean anySet(Object baseObject, long baseOffset, long bitSetWidthInWords) {
    long addr = baseOffset;
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
