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

package org.apache.spark.memory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * Manages the memory allocated by an individual task.
 *
 *  用于管理分配给单个任务的内存
 *
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 *
 *  在off-heap模式下，内存由64位的long值直接进行寻址；在on-heap模式下，内存由对象+对象内offset进行寻址
 *
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 *
 * 这个问题说的是啥？何为record pointer?
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 *
 * 对于on heap，
 * 1. 使用高13位记录page number，一共可以有2^13=8k=8*1024=8192页
 * 2. 使用低51位记录页面内的偏移量
 * 3.
 *
 * page size是由long[] 的最大值决定
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * 2^32 * 8 bytes, which is
 * approximately 35 terabytes of memory.
 */
public class TaskMemoryManager {

  private final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of bits used to encode offsets in data pages. */
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /** The number of entries in the page table.，为2^13=8192 */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's maximum page
   * size is limited by the maximum amount of data that can be stored in a  long[] array, which is
   * (2^32 - 1) * 8 bytes (or 16 gigabytes). Therefore, we cap this at 16 gigabytes.
   *
   * 堆上每页最大的字节数由long数组决定，因为数组下标为整数，因此long数组的最大长度是2^32 - 1，long数组占用的字节数是
   *  (2^32 - 1)*8
   *
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /** Bit mask for the lower 51 bits of a long. ,低51位
   * */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /** Bit mask for the upper 13 bits of a long */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   *
   * 因为page number是13bit，那么PAGE_TABLE_SIZE也就是2^13 = 8192
   *
   * 也就是说，每个TaskMemoryManager持有的MemoryBlock是8192个
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   *
   * Java的BitSet的用法什么？构造参数是什么含义？
   *
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  /***
   * TaskMemoryManager持有一个MemoryManager
   */
  private final MemoryManager memoryManager;

  /***
   * 因为TaskMemoryManager是与Task相关的内存管理器，因此TaskMemoryManager持有一个TaskAttemptID
   */
  private final long taskAttemptId;

  /**
   * Tracks whether we're in-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   *
   * tungstenMemoryMode用于标识MemoryMode，目前共有两种Mode，ON_HEAP和OFF_HEAP，
   * 注意：这是一个final的变量，也就是构造完成后不能再修改
   */
  final MemoryMode tungstenMemoryMode;

  /**
   * Tracks spillable memory consumers.
   *
   * TaskMemoryManager持有一个MemoryConsumer集合，MemoryConsumer的实现类有：
   * UnsafeExternalSort
   * ShuffleExternalSort
   * BytesToBytesMap
   */
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  /**
   * TaskMemoryManager的构造函数
   *
   * @param memoryManager
   * @param taskAttemptId
   */
  public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
    /***
     * 从MemoryManager得到MemoryMode
     */
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.taskAttemptId = taskAttemptId;
    this.consumers = new HashSet<>();
  }

  /**
   *
   * 为memory consumer申请required字节的内存，如果没有足够的内存，它将调用consumer的spill方法将数据转储到磁盘
   * 问题：为什么把spill动作交给TaskMemoryManager来执行？TaskMemoryManager仅仅负责内存管理，并且在内存申请不到时是否spill应该
   * 由consumer自行决定
   *
   * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
   * spill() of consumers to release more memory.
   *
   * @return number of bytes successfully granted (<= N).
   *
   * @param required 申请的字节数，实际申请到的字节数(返回值)可能小于required
   * @param mode
   * @param consumer
   * @return
   */
  public long acquireExecutionMemory(
      long required,
      MemoryMode mode,
      MemoryConsumer consumer) {
    assert(required >= 0);
    // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
    // memory here, then it may not make sense to spill since that would only end up freeing
    // off-heap memory. This is subject to change, though, so it may be risky to make this
    // optimization now in case we forget to undo it late when making changes.
    /***
     *  根据不同的MemoryMode执行不同的分配策略，是如何体现的？
     *  调用MemoryManager的acquireExecutionMemory时，会根据不同的mode进行判断
     *  如果是on heap模式，execution可能想storage借用内存；如果是off heap模式，那么内存不会向storage借用内存(也就是说，在off heap模式下，
     *  execution和storage是绝缘的)
     */
    synchronized (this) {

      /***
       * 申请到了acquired字节的内存
       */
      long acquired = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

      /***
       * Try to release memory from other consumers first, then we can reduce the frequency of
       * spilling, avoid to have too many spilled files.
       *
       * 在调用自身(MemoryConsumer)的spill方法之前，首先尝试release其它consumer的内存
       */
      if (acquired < required) {
        // Call spill() on other consumers to release memory
        for (MemoryConsumer c: consumers) {

          /***
           * 遍历consumers,如果当前遍历到的consumer
           */
          if (c != consumer && c.getUsed() > 0) {
            try {

              /**
               * 为consumer释放required - acquired字节的内存空间,交还给MemoryManager
               */
              long released = c.spill(required - acquired, consumer);

              /***
               *  如果consumer释放了内存空间且当前mode是tungstenMemoryMode
               */
              if (released > 0 && mode == tungstenMemoryMode) {
                logger.debug("Task {} released {} from {} for {}", taskAttemptId,
                  Utils.bytesToString(released), c, consumer);

                /***
                 * 再次调用memoryManager的acquireExecutionMemory方法申请内存，将它累加到acquired上
                 */
                acquired += memoryManager.acquireExecutionMemory(required - acquired, taskAttemptId, mode);

                /***
                 * 如果所需的内存都已经申请到，那么就跳出循环
                 */
                if (acquired >= required) {
                  break;
                }
              }
            } catch (IOException e) {
              logger.error("error while calling spill() on " + c, e);
              throw new OutOfMemoryError("error while calling spill() on " + c + " : "
                + e.getMessage());
            }
          }
        }
      }

      /***
       *  call spill() on itself
       * 遍历完所有的其它consumers，还没有满足申请到的内存，那么调用自身的spill操作进行转储磁盘的操作
       */
      if (acquired < required && consumer != null) {
        try {
          /***
           *  释放released字节的内存
           */
          long released = consumer.spill(required - acquired, consumer);

          /***
           * 如果consumer释放了一些空间，那么调用memoryManager.acquireExecutionMemory再次申请内存并将申请到内存空间累加到acquired上
           */
          if (released > 0 && mode == tungstenMemoryMode) {
            logger.debug("Task {} released {} from itself ({})", taskAttemptId,
              Utils.bytesToString(released), consumer);
            acquired += memoryManager.acquireExecutionMemory(required - acquired, taskAttemptId, mode);
          }
        } catch (IOException e) {
          logger.error("error while calling spill() on " + consumer, e);
          throw new OutOfMemoryError("error while calling spill() on " + consumer + " : "
            + e.getMessage());
        }
      }

      /***
       * 这个地方是否会有重复操作，如果consumers已经持有了consumer了呢？虽然代码没错(因为consumers是个HashSet)
       */
      if (consumer != null) {
        consumers.add(consumer);
      }
      logger.debug("Task {} acquire {} for {}", taskAttemptId, Utils.bytesToString(acquired), consumer);
      return acquired;
    }
  }

  /***
   * Release N bytes of execution memory for a MemoryConsumer.
   *
   * 释放consumer使用的size字节的内存
   *
   * @param size
   * @param mode
   * @param consumer
   */
  public void releaseExecutionMemory(long size, MemoryMode mode, MemoryConsumer consumer) {
    logger.debug("Task {} release {} from {}", taskAttemptId, Utils.bytesToString(size), consumer);
    memoryManager.releaseExecutionMemory(size, taskAttemptId, mode);
  }

  /**
   * Dump the memory usage of all consumers.
   *
   * 列出所有consumer的内存使用情况
   */
  public void showMemoryUsage() {
    logger.info("Memory used in task " + taskAttemptId);
    synchronized (this) {
      long memoryAccountedForByConsumers = 0;
      for (MemoryConsumer c: consumers) {
        long totalMemUsage = c.getUsed();
        memoryAccountedForByConsumers += totalMemUsage;
        if (totalMemUsage > 0) {
          logger.info("Acquired by " + c + ": " + Utils.bytesToString(totalMemUsage));
        }
      }
      long memoryNotAccountedFor =
        memoryManager.getExecutionMemoryUsageForTask(taskAttemptId) - memoryAccountedForByConsumers;
      logger.info(
        "{} bytes of memory were used by task {} but are not associated with specific consumers",
        memoryNotAccountedFor, taskAttemptId);
      logger.info(
        "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
        memoryManager.executionMemoryUsed(), memoryManager.storageMemoryUsed());
    }
  }

  /**
   * Return the page size in bytes.
   */
  public long pageSizeBytes() {
    return memoryManager.pageSizeBytes();
  }

  /**
   *  分配一个page的内存
   *
   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
   * intended for allocating large blocks of Tungsten memory that will be shared between operators.
   *
   *  分配一整块内存空间，由TaskMemoryManager的pageTable进行跟踪和追溯。一下子分配一块大内存的目的是为了在多个operator之间共用
   *
   * Returns `null` if there was not enough memory to allocate the page. May return a page that
   * contains fewer bytes than requested, so callers should verify the size of returned pages.
   *
   *  如果没有足够的内存进行分配，那么将返回null；返回的page的大小可能比申请的字节数(size)小
   *
   * @param size
   * @param consumer
   * @return
   */
  public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {

    /***
     * 如果申请的内存大于一个page代表的内存空间，那么将报错；一个page能代表的内存空间是16G字节
     */
    if (size > MAXIMUM_PAGE_SIZE_BYTES) {
      throw new IllegalArgumentException(
        "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
    }

    /***
     * 调用acquireExecutionMemory申请内存，返回申请的字节数
     */
    long acquired = acquireExecutionMemory(size, tungstenMemoryMode, consumer);

    /***
     * 如果申请到的字节数小于等于0，那么将返回null
     */
    if (acquired <= 0) {
      return null;
    }

    final int pageNumber;
    synchronized (this) {
      pageNumber = allocatedPages.nextClearBit(0);
      if (pageNumber >= PAGE_TABLE_SIZE) {
        releaseExecutionMemory(acquired, tungstenMemoryMode, consumer);
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      allocatedPages.set(pageNumber);
    }
    final MemoryBlock page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
    page.pageNumber = pageNumber;
    pageTable[pageNumber] = page;
    if (logger.isTraceEnabled()) {
      logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
    }
    return page;
  }

  /**
   * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}.
    *
   * 释放一个内存page
   * @param page
   * @param consumer
   */
  public void freePage(MemoryBlock page, MemoryConsumer consumer) {

    /***
     * page的pageNumber不能为-1，-1表示该page不是由allocatePage分配的(-1是pageNumber在MemoryBlock中的默认值)
     */
    assert (page.pageNumber != -1) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";


    assert(allocatedPages.get(page.pageNumber));

    /***
     * 向pageTable数组下标为page.pageNumber的元素置为null
     */
    pageTable[page.pageNumber] = null;
    synchronized (this) {
      allocatedPages.clear(page.pageNumber);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }

    /***
     * 该page对应的内存的实际大小
     */
    long pageSize = page.size();

    /***
     * 释放page占用的内存，这个是物理释放
     */
    memoryManager.tungstenMemoryAllocator().free(page);

    /***
     * 释放consumer使用的内存
     */
    releaseExecutionMemory(pageSize, tungstenMemoryMode, consumer);
  }

  /**
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   *
   *
   *
   * @param page a data page allocated by {@link TaskMemoryManager#allocatePage}/
   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
   *                     this should be the value that you would pass as the base offset into an
   *                     UNSAFE call (e.g. page.baseOffset() + something).
   * @return an encoded page address. 返回的是page address还是record address？
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
      // encode. Due to our page size limitation, though, we can convert this into an offset that's
      // relative to the page's base offset; this relative offset will fit in 51 bits.
      offsetInPage -= page.getBaseOffset();
    }
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
  }

  @VisibleForTesting
  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
  }

  @VisibleForTesting
  public static int decodePageNumber(long pagePlusOffsetAddress) {
    return (int) ((pagePlusOffsetAddress & MASK_LONG_UPPER_13_BITS) >>> OFFSET_BITS);
  }

  private static long decodeOffset(long pagePlusOffsetAddress) {
    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
  }

  /**
   * Get the page associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public Object getPage(long pagePlusOffsetAddress) {
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      assert (page.getBaseObject() != null);
      return page.getBaseObject();
    } else {
      return null;
    }
  }

  /**
   * Get the offset associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      return offsetInPage;
    } else {
      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
      // converted the absolute address into a relative address. Here, we invert that operation:
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      return page.getBaseOffset() + offsetInPage;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   *
   * 释放为该Task分配的所有内存
   */
  public long cleanUpAllAllocatedMemory() {
    synchronized (this) {
      /***
       * 将pageTable的所有元素置为null
       */
      Arrays.fill(pageTable, null);

      /***
       * 遍历所有的consumers
       */
      for (MemoryConsumer c: consumers) {
        if (c != null && c.getUsed() > 0) {
          // In case of failed task, it's normal to see leaked memory
          logger.warn("leak " + Utils.bytesToString(c.getUsed()) + " memory from " + c);
        }
      }
      consumers.clear();
    }

    /***
     * 为什么会有不为null的page
     */
    for (MemoryBlock page : pageTable) {
      if (page != null) {
        memoryManager.tungstenMemoryAllocator().free(page);
      }
    }
    Arrays.fill(pageTable, null);

    return memoryManager.releaseAllExecutionMemoryForTask(taskAttemptId);
  }

  /**
   * Returns the memory consumption, in bytes, for the current task.
   * 获取当前任务已经申请到的内存
   */
  public long getMemoryConsumptionForThisTask() {
    return memoryManager.getExecutionMemoryUsageForTask(taskAttemptId);
  }
}
