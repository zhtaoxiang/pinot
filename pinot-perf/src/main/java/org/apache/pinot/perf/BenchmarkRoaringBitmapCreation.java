/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkRoaringBitmapCreation {

  private static final int NUM_DOCS = 1_000_000;
  private static final int NUM_READS = 10000;
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "bitmap_creation_benchmark_" + System.currentTimeMillis());

  @Param({"1000", "10000", "100000"}) // 1k, 10k, 100k
  public int _cardinality;

  private int _numBitmaps;
  private BitmapInvertedIndexWriter _bitmapInvertedIndexWriter;
  private SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps;
  private PinotDataBuffer _offsetBuffer;
  private PinotDataBuffer _bitmapBuffer;
  private int _firstOffset;
  private int[] _dictIdsToQuery;

  @Setup
  public void setup()
      throws IllegalAccessException, InstantiationException, IOException {
    _numBitmaps = _cardinality;
    File bufferDir = new File(TEMP_DIR, "cardinality_" + _cardinality);
    FileUtils.forceMkdir(bufferDir);
    File bufferFile = new File(bufferDir, "buffer");
    _bitmapInvertedIndexWriter = new BitmapInvertedIndexWriter(bufferFile, _numBitmaps);
    // Insert between 10-1000 values per bitmap
    for (int i = 0; i < _numBitmaps; i++) {
      int size = 10 + RandomUtils.nextInt(990);
      int[] data = new int[size];
      for (int j = 0; j < size; j++) {
        data[j] = RandomUtils.nextInt(NUM_DOCS); // docIds will repeat across bitmaps, but doesn't matter for purpose of this benchmark
      }
      RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
      _bitmapInvertedIndexWriter.add(bitmap);
    }
    PinotDataBuffer dataBuffer = PinotByteBuffer.mapReadOnlyBigEndianFile(bufferFile);
    long offsetBufferEndOffset = (long) (_numBitmaps + 1) * Integer.BYTES;
    _offsetBuffer = dataBuffer.view(0, offsetBufferEndOffset, ByteOrder.BIG_ENDIAN);
    _bitmapBuffer = dataBuffer.view(offsetBufferEndOffset, dataBuffer.size());
    _firstOffset = _offsetBuffer.getInt(0);

    // A fixed set of dictIds to read. This ensures same bitmap accessed multiple times.
    _dictIdsToQuery = new int[100];
    for (int i = 0; i < 100; i++) {
      _dictIdsToQuery[i] = RandomUtils.nextInt(_cardinality);
    }
  }

  @TearDown
  public void teardown()
      throws IOException {
    _bitmapInvertedIndexWriter.close();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void cacheReferences() {
    _bitmaps = null;
    for (int i = 0; i < NUM_READS; i++) {
      int dictId = _dictIdsToQuery[RandomUtils.nextInt(_dictIdsToQuery.length)];
      getRoaringBitmapFromCache(dictId);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void alwaysBuild() {
    for (int i = 0; i < NUM_READS; i++) {
      int dictId = _dictIdsToQuery[RandomUtils.nextInt(_dictIdsToQuery.length)];
      buildRoaringBitmap(dictId);
    }
  }

  private ImmutableRoaringBitmap getRoaringBitmapFromCache(int dictId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = (_bitmaps != null) ? _bitmaps.get() : null;
    if (bitmapArrayReference != null) {
      SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[dictId];
      ImmutableRoaringBitmap bitmap = (bitmapReference != null) ? bitmapReference.get() : null;
      if (bitmap != null) {
        return bitmap;
      }
    } else {
      bitmapArrayReference = new SoftReference[_numBitmaps];
      _bitmaps = new SoftReference<>(bitmapArrayReference);
    }
    synchronized (this) {
      SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[dictId];
      ImmutableRoaringBitmap bitmap = (bitmapReference != null) ? bitmapReference.get() : null;
      if (bitmap == null) {
        bitmap = buildRoaringBitmap(dictId);
        bitmapArrayReference[dictId] = new SoftReference<>(bitmap);
      }
      return bitmap;
    }
  }

  private ImmutableRoaringBitmap buildRoaringBitmap(int dictId) {
    int offset = _offsetBuffer.getInt(dictId * Integer.BYTES);
    int length = _offsetBuffer.getInt((dictId + 1) * Integer.BYTES) - offset;
    return new ImmutableRoaringBitmap(_bitmapBuffer.toDirectByteBuffer(offset - _firstOffset, length));
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkRoaringBitmapCreation.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10)).warmupIterations(1).measurementTime(TimeValue.seconds(5))
        .measurementIterations(1).forks(1);
    new Runner(opt.build()).run();
  }
}
