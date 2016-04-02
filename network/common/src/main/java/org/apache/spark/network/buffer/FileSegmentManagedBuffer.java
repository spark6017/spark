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

package org.apache.spark.network.buffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import io.netty.channel.DefaultFileRegion;

import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 *
 * FileSegmentManagedBuffer的底层存储是由一个分段的数据文件+数据在文件中的起始位置以及数据长度表示
 *
 * 注意：FileSegmentManagedBuffer本身并没有记录该segment的所有者
 *
 */
public final class FileSegmentManagedBuffer extends ManagedBuffer {
  private final TransportConf conf;
  private final File file;
  private final long offset;
  private final long length;

  /***
   *
   * @param conf
   * @param file 聚合了所有reducer(分段)的数据文件
   * @param offset reducer数据在数据文件中的其实位置
   * @param length reducer数据的长度
   */
  public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
    this.conf = conf;
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  /***
   * 该ManagedBuffer的数据长度
   * @return
   */
  @Override
  public long size() {
    return length;
  }

  /***
   * 将FileSegmentManagedBuffer中的数据转换为ByteBuffer
   * @return
   * @throws IOException
   */
  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    FileChannel channel = null;
    try {
      /***
       * 打开随机访问文件通道
       */
      channel = new RandomAccessFile(file, "r").getChannel();

      /***
       *   Just copy the buffer if it's sufficiently small, as memory mapping has a high overhead.
       *   如果数据量很小，那么不启用内存映射的方式进行读取(因为内存映射的代价比较大，为了小文件进行内存映射，得不偿失)
       *   配置项spark.storage.memoryMapThreshold，默认值是2M
       *   也就是说，如果要读取的数据量小于2M，那么进行常规的IO操作
       */
      if (length < conf.memoryMapBytes()) {
        /***
         * 直接分配空间
         */
        ByteBuffer buf = ByteBuffer.allocate((int) length);

        /***
         * 定位到要读取的位置
         */
        channel.position(offset);

        /***
         * 如果buf还没有填充满，则一直进行循环填充
         */
        while (buf.remaining() != 0) {
          /***
           * 问题：channel.read一次read会填充多少字节？
           */
          if (channel.read(buf) == -1) {
            throw new IOException(String.format("Reached EOF before filling buffer\n" +
              "offset=%s\nfile=%s\nbuf.remaining=%s",
              offset, file.getAbsoluteFile(), buf.remaining()));
          }
        }
        /***
         * 将buf复位到可用状态
         */
        buf.flip();
        return buf;
      } else {

        /***
         * 通过内存映射方式，从offset处开始读取，读取length个数据，
         * 这是一个API简单，但是很高效的操作
         */
        return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
      }
    } catch (IOException e) {
      try {
        if (channel != null) {
          /***
           * FileChannel的size操作返回的文件的字节数，
           */
          long size = channel.size();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
            e);
        }
      } catch (IOException ignored) {
        // ignore
      }
      throw new IOException("Error in opening " + this, e);
    } finally {
      /***
       * 关闭文件通道
       */
      JavaUtils.closeQuietly(channel);
    }
  }

  /***
   * 返回一个InputStream，该InputStream包含了该segment的数据，可以直接读取
   * @return
   * @throws IOException
   */
  @Override
  public InputStream createInputStream() throws IOException {
    FileInputStream is = null;
    try {
      is = new FileInputStream(file);
      /***
       * 跳过offset个字节，然后构造LimitedInputStream，大小为length
       */
      ByteStreams.skipFully(is, offset);
      return new LimitedInputStream(is, length);
    } catch (IOException e) {
      try {
        if (is != null) {
          long size = file.length();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
              e);
        }
      } catch (IOException ignored) {
        // ignore
      } finally {
        JavaUtils.closeQuietly(is);
      }
      throw new IOException("Error in opening " + this, e);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(is);
      throw e;
    }
  }

  /**
   * 何为retain？
   * @return
   */
  @Override
  public ManagedBuffer retain() {
    return this;
  }

  /***
   * 何为release？
   * @return
   */
  @Override
  public ManagedBuffer release() {
    return this;
  }

  /***
   * 转换为Netty
   * @return
   * @throws IOException
   */
  @Override
  public Object convertToNetty() throws IOException {
    /***
     * 配置项io.lazyFD
     */
    if (conf.lazyFileDescriptor()) {
      /***
       * 传入三个参数，文件、数据在文件中的偏移量以及数据长度
       */
      return new LazyFileRegion(file, offset, length);
    } else {

      /***
       * 打开文件通道，构造DefaultFileRegion，DefaultFileRegion是Netty的API
       */
      FileChannel fileChannel = new FileInputStream(file).getChannel();
      return new DefaultFileRegion(fileChannel, offset, length);
    }
  }

  public File getFile() { return file; }

  public long getOffset() { return offset; }

  public long getLength() { return length; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("file", file)
      .add("offset", offset)
      .add("length", length)
      .toString();
  }
}
