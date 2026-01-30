package org.test.boxfs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for thread safety and concurrent access guarantees.
 */
class ConcurrencyTest {

  @TempDir
  Path tempDir;

  private FileSystem fs;

  @BeforeEach
  void setUp() throws IOException {
    Path container = tempDir.resolve("concurrent.box");
    var uri = URI.create("box:" + container);
    fs = FileSystems.newFileSystem(uri, Map.of("create", "true", "totalBlocks", 512L));
  }

  @AfterEach
  void tearDown() throws IOException {
    if (fs != null && fs.isOpen()) {
      fs.close();
    }
  }

  /**
   * Verifies that concurrent writes to the same file position don't interleave bytes.
   * Each thread writes a distinct pattern; the result must be one complete pattern.
   */
  @Test
  void concurrentWritesAreAtomic() throws Exception {
    var file = fs.getPath("/atomic.bin");
    int dataSize = 1000;
    int threadCount = 10;

    // Pre-create file
    Files.write(file, new byte[dataSize]);

    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new AtomicBoolean(false);

    for (int t = 0; t < threadCount; t++) {
      byte pattern = (byte) ('A' + t);
      var data = new byte[dataSize];
      Arrays.fill(data, pattern);

      new Thread(() -> {
        try {
          startLatch.await();
          try (var channel = Files.newByteChannel(file, StandardOpenOption.WRITE)) {
            channel.position(0);
            channel.write(ByteBuffer.wrap(data));
          }
        } catch (Exception e) {
          errors.set(true);
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    // Start all threads simultaneously
    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads should complete");
    assertFalse(errors.get(), "No errors should occur");

    // Verify: all bytes must be the same (one complete pattern, no interleaving)
    var result = Files.readAllBytes(file);
    byte firstByte = result[0];

    // First byte must be one of our patterns
    assertTrue(firstByte >= 'A' && firstByte < 'A' + threadCount,
      "Result should be one of the written patterns");

    // All bytes must match (no interleaving)
    for (int i = 0; i < result.length; i++) {
      assertEquals(firstByte, result[i],
        "Byte at position " + i + " differs - interleaving detected!");
    }
  }

  /**
   * Verifies that reads during concurrent writes see consistent data using SeekableByteChannel.
   * Uses explicit channel operations (not Files.write) to ensure atomic writes under our lock.
   */
  @Test
  void readsAreConsistentDuringWrites() throws Exception {
    var file = fs.getPath("/consistent.bin");
    int dataSize = 500;

    // Initial content: all zeros
    Files.write(file, new byte[dataSize]);

    int iterations = 100;
    var errors = new AtomicBoolean(false);
    var inconsistencies = new AtomicInteger(0);
    var done = new AtomicBoolean(false);

    // Writer thread - uses channel directly for atomic writes
    var writerThread = new Thread(() -> {
      try {
        for (int i = 0; i < iterations && !errors.get(); i++) {
          byte pattern = (i % 2 == 0) ? (byte) 'X' : (byte) 'Y';
          var data = new byte[dataSize];
          Arrays.fill(data, pattern);
          // Use channel with WRITE only (no truncate) for atomic operation
          try (var channel = Files.newByteChannel(file, StandardOpenOption.WRITE)) {
            channel.position(0);
            channel.write(ByteBuffer.wrap(data));
          }
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      } finally {
        done.set(true);
      }
    });

    // Reader thread - uses a channel directly for atomic reads
    var readerThread = new Thread(() -> {
      try {
        while (!done.get() && !errors.get()) {
          try (var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
            var buffer = ByteBuffer.allocate(dataSize);
            channel.read(buffer);
            var result = buffer.array();

            if (result.length > 0) {
              byte first = result[0];
              for (byte b : result) {
                // All bytes should be the same (0, 'X', or 'Y')
                if (b != first) {
                  inconsistencies.incrementAndGet();
                  break;
                }
              }
            }
          }
          Thread.sleep(1);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      }
    });

    writerThread.start();
    readerThread.start();

    writerThread.join(10000);
    readerThread.join(10000);

    assertFalse(errors.get(), "No errors should occur");
    assertEquals(0, inconsistencies.get(),
      "All reads should see consistent data (all same byte)");
  }
}
