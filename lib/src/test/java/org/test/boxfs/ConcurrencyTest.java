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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
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
   * Verifies that multiple readers can read concurrently without blocking each other.
   */
  @Test
  void multipleReadersCanReadConcurrently() throws Exception {
    var file = fs.getPath("/readers.txt");
    var content = "Concurrent read test content";
    Files.write(file, content.getBytes());

    int readerCount = 10;
    var readyLatch = new CountDownLatch(readerCount);
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(readerCount);
    var results = new ArrayList<String>();
    var errors = new AtomicBoolean(false);

    for (int i = 0; i < readerCount; i++) {
      new Thread(() -> {
        try {
          readyLatch.countDown(); // Signal thread is ready
          startLatch.await();     // Wait for start signal

          // All threads read simultaneously
          var bytes = Files.readAllBytes(file);
          var result = new String(bytes);

          synchronized (results) {
            results.add(result);
          }
        } catch (Exception e) {
          errors.set(true);
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    // Wait for all threads to be ready, then start them together
    assertTrue(readyLatch.await(5, TimeUnit.SECONDS), "All threads should be ready");
    startLatch.countDown();

    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All readers should complete");
    assertFalse(errors.get(), "No errors should occur");

    // All readers should get the same content
    assertEquals(readerCount, results.size());
    for (var result : results) {
      assertEquals(content, result);
    }
  }

  /**
   * Verifies that writes to different files are also serialized (filesystem-level lock).
   * Each thread writes to a different file; we verify no errors occur.
   */
  @Test
  void concurrentWritesToDifferentFiles() throws Exception {
    int threadCount = 10;
    int dataSize = 500;

    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new AtomicBoolean(false);

    for (int t = 0; t < threadCount; t++) {
      int fileNum = t;
      byte pattern = (byte) ('A' + t);
      var data = new byte[dataSize];
      Arrays.fill(data, pattern);

      new Thread(() -> {
        try {
          startLatch.await();
          var file = fs.getPath("/file" + fileNum + ".bin");
          Files.write(file, data);
        } catch (Exception e) {
          errors.set(true);
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads should complete");
    assertFalse(errors.get(), "No errors should occur");

    // Verify each file has correct content
    for (int t = 0; t < threadCount; t++) {
      var file = fs.getPath("/file" + t + ".bin");
      var result = Files.readAllBytes(file);
      assertEquals(dataSize, result.length);

      byte expected = (byte) ('A' + t);
      for (byte b : result) {
        assertEquals(expected, b);
      }
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

  /**
   * Stress test: many threads doing mixed read/write operations.
   * Verifies no exceptions and data integrity.
   */
  @Test
  void stressMixedReadWrite() throws Exception {
    var file = fs.getPath("/stress.bin");
    int dataSize = 100;

    // Initialize with a known pattern
    var initialData = new byte[dataSize];
    Arrays.fill(initialData, (byte) 'Z');
    Files.write(file, initialData);

    int threadCount = 20;
    int operationsPerThread = 50;
    var errors = new AtomicInteger(0);
    var doneLatch = new CountDownLatch(threadCount);

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (int t = 0; t < threadCount; t++) {
        byte pattern = (byte) ('A' + (t % 26));
        executor.submit(() -> {
          try {
            for (int op = 0; op < operationsPerThread; op++) {
              if (op % 3 == 0) {
                // Write operation using channel directly for atomicity
                var data = new byte[dataSize];
                Arrays.fill(data, pattern);
                try (var channel = Files.newByteChannel(file,
                  StandardOpenOption.WRITE)) {
                  channel.position(0);
                  channel.write(ByteBuffer.wrap(data));
                }
              } else {
                // Read operation using channel directly
                try (var channel = Files.newByteChannel(file,
                  StandardOpenOption.READ)) {
                  var buf = ByteBuffer.allocate(dataSize);
                  channel.read(buf);
                  var result = buf.array();

                  // Verify no partial writes (all bytes must be same)
                  byte first = result[0];
                  for (int i = 1; i < result.length; i++) {
                    if (result[i] != first) {
                      // Interleaving detected!
                      errors.incrementAndGet();
                      break;
                    }
                  }
                }
              }
            }
          } catch (Exception e) {
            errors.incrementAndGet();
            e.printStackTrace();
          } finally {
            doneLatch.countDown();
          }
        });
      }

      assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete");
    }

    assertEquals(0, errors.get(), "No errors or data corruption should occur");
  }

  // ==================== Directory Operations During File Operations ====================

  /**
   * Verifies that directory listing remains consistent while files are being created.
   */
  @Test
  void directoryListingDuringFileCreation() throws Exception {
    var dir = fs.getPath("/listing-test");
    Files.createDirectory(dir);

    int fileCount = 20;
    var errors = new AtomicBoolean(false);
    var creatorDone = new AtomicBoolean(false);

    // Creator thread - creates files
    var creatorThread = new Thread(() -> {
      try {
        for (int i = 0; i < fileCount; i++) {
          Files.write(dir.resolve("file" + i + ".txt"), ("content" + i).getBytes());
          Thread.sleep(5);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      } finally {
        creatorDone.set(true);
      }
    });

    // Lister thread - repeatedly lists directory
    var listerThread = new Thread(() -> {
      try {
        while (!creatorDone.get() && !errors.get()) {
          try (var stream = Files.newDirectoryStream(dir)) {
            // Just iterate - should not throw ConcurrentModificationException
            for (var entry : stream) {
              assertNotNull(entry.getFileName());
            }
          }
          Thread.sleep(2);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      }
    });

    creatorThread.start();
    listerThread.start();

    creatorThread.join(10000);
    listerThread.join(10000);

    assertFalse(errors.get(), "No errors should occur during concurrent listing and creation");
  }

  /**
   * Verifies that directory listing remains consistent while files are being deleted.
   */
  @Test
  void directoryListingDuringFileDeletion() throws Exception {
    var dir = fs.getPath("/deletion-test");
    Files.createDirectory(dir);

    // Pre-create files
    int fileCount = 20;
    for (int i = 0; i < fileCount; i++) {
      Files.write(dir.resolve("file" + i + ".txt"), ("content" + i).getBytes());
    }

    var errors = new AtomicBoolean(false);
    var deleterDone = new AtomicBoolean(false);

    // Deleter thread - deletes files
    var deleterThread = new Thread(() -> {
      try {
        for (int i = 0; i < fileCount; i++) {
          var file = dir.resolve("file" + i + ".txt");
          if (Files.exists(file)) {
            Files.delete(file);
          }
          Thread.sleep(5);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      } finally {
        deleterDone.set(true);
      }
    });

    // Lister thread - repeatedly lists directory
    var listerThread = new Thread(() -> {
      try {
        while (!deleterDone.get() && !errors.get()) {
          try (var stream = Files.newDirectoryStream(dir)) {
            for (var entry : stream) {
              assertNotNull(entry.getFileName());
            }
          }
          Thread.sleep(2);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      }
    });

    deleterThread.start();
    listerThread.start();

    deleterThread.join(10000);
    listerThread.join(10000);

    assertFalse(errors.get(), "No errors should occur during concurrent listing and deletion");
  }

  // ==================== Channel Lifecycle ====================

  /**
   * Verifies that multiple channels can be open to the same file simultaneously.
   */
  @Test
  void multipleChannelsToSameFile() throws Exception {
    var file = fs.getPath("/multi-channel.bin");
    int dataSize = 100;
    Files.write(file, new byte[dataSize]);

    int channelCount = 5;
    var errors = new AtomicBoolean(false);
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(channelCount);

    for (int c = 0; c < channelCount; c++) {
      byte pattern = (byte) ('A' + c);
      new Thread(() -> {
        try {
          startLatch.await();
          try (var channel = Files.newByteChannel(file,
            StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // Write pattern
            var writeData = new byte[dataSize];
            Arrays.fill(writeData, pattern);
            channel.position(0);
            channel.write(ByteBuffer.wrap(writeData));

            // Read back
            channel.position(0);
            var readBuffer = ByteBuffer.allocate(dataSize);
            channel.read(readBuffer);
          }
        } catch (Exception e) {
          errors.set(true);
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All channels should complete");
    assertFalse(errors.get(), "No errors with multiple channels");
  }

  /**
   * Verifies behavior when truncating via one channel while reading through another.
   */
  @Test
  void truncateWhileReading() throws Exception {
    var file = fs.getPath("/truncate-read.bin");
    int dataSize = 1000;
    var initialData = new byte[dataSize];
    Arrays.fill(initialData, (byte) 'X');
    Files.write(file, initialData);

    var errors = new AtomicBoolean(false);
    var truncaterDone = new AtomicBoolean(false);

    // Truncater thread
    var truncaterThread = new Thread(() -> {
      try {
        for (int i = 0; i < 10; i++) {
          try (var channel = Files.newByteChannel(file,
            StandardOpenOption.WRITE)) {
            channel.truncate(dataSize / 2);
          }
          // Restore size
          try (var channel = Files.newByteChannel(file,
            StandardOpenOption.WRITE)) {
            channel.position(dataSize - 1);
            channel.write(ByteBuffer.wrap(new byte[]{0}));
          }
          Thread.sleep(10);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      } finally {
        truncaterDone.set(true);
      }
    });

    // Reader thread
    var readerThread = new Thread(() -> {
      try {
        while (!truncaterDone.get() && !errors.get()) {
          try (var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
            var buffer = ByteBuffer.allocate(dataSize);
            channel.read(buffer);
            // Read should complete without error
          }
          Thread.sleep(5);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      }
    });

    truncaterThread.start();
    readerThread.start();

    truncaterThread.join(10000);
    readerThread.join(10000);

    assertFalse(errors.get(), "No errors during truncate while reading");
  }

  // ==================== File Operations During Reads/Writes ====================

  /**
   * Verifies behavior when deleting a file that is being read.
   * The delete should either succeed (and reads fail) or block until read completes.
   */
  @Test
  void deleteFileWhileReading() throws Exception {
    var file = fs.getPath("/delete-while-read.bin");
    Files.write(file, "test content for deletion".getBytes());

    var readStarted = new CountDownLatch(1);
    var deleteAttempted = new AtomicBoolean(false);
    var errors = new AtomicInteger(0);

    // Reader thread - holds file open
    var readerThread = new Thread(() -> {
      try (var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
        readStarted.countDown();
        // Hold the channel open while delete is attempted
        Thread.sleep(100);
        // Try to read - may or may not succeed depending on delete timing
        try {
          var buffer = ByteBuffer.allocate(100);
          channel.read(buffer);
        } catch (IOException e) {
          // Expected if file was deleted
        }
      } catch (Exception e) {
        errors.incrementAndGet();
        e.printStackTrace();
      }
    });

    // Deleter thread
    var deleterThread = new Thread(() -> {
      try {
        readStarted.await();
        Thread.sleep(20); // Let reader start
        try {
          Files.delete(file);
          deleteAttempted.set(true);
        } catch (IOException e) {
          // Delete might fail if file is in use - that's acceptable
          deleteAttempted.set(true);
        }
      } catch (Exception e) {
        errors.incrementAndGet();
        e.printStackTrace();
      }
    });

    readerThread.start();
    deleterThread.start();

    readerThread.join(5000);
    deleterThread.join(5000);

    assertTrue(deleteAttempted.get(), "Delete should have been attempted");
    assertEquals(0, errors.get(), "No unexpected errors");
  }

  /**
   * Verifies behavior when moving/renaming a file while it's open.
   */
  @Test
  void moveFileWhileOpen() throws Exception {
    var source = fs.getPath("/move-source.bin");
    var target = fs.getPath("/move-target.bin");
    Files.write(source, "content to move".getBytes());

    var channelOpened = new CountDownLatch(1);
    var moveAttempted = new AtomicBoolean(false);
    var errors = new AtomicInteger(0);

    // Holder thread - keeps file open
    var holderThread = new Thread(() -> {
      try (var channel = Files.newByteChannel(source, StandardOpenOption.READ)) {
        channelOpened.countDown();
        Thread.sleep(100);
        // Try to read after potential move
        try {
          var buffer = ByteBuffer.allocate(100);
          channel.read(buffer);
        } catch (IOException e) {
          // May fail if file was moved
        }
      } catch (Exception e) {
        errors.incrementAndGet();
        e.printStackTrace();
      }
    });

    // Mover thread
    var moverThread = new Thread(() -> {
      try {
        channelOpened.await();
        Thread.sleep(20);
        try {
          Files.move(source, target);
          moveAttempted.set(true);
        } catch (IOException e) {
          // Move might fail if file is in use - acceptable
          moveAttempted.set(true);
        }
      } catch (Exception e) {
        errors.incrementAndGet();
        e.printStackTrace();
      }
    });

    holderThread.start();
    moverThread.start();

    holderThread.join(5000);
    moverThread.join(5000);

    assertTrue(moveAttempted.get(), "Move should have been attempted");
    assertEquals(0, errors.get(), "No unexpected errors");
  }

  /**
   * Verifies that reading file attributes is consistent during writes.
   */
  @Test
  void readAttributesWhileWriting() throws Exception {
    var file = fs.getPath("/attrs-during-write.bin");
    Files.write(file, new byte[100]);

    var errors = new AtomicBoolean(false);
    var writerDone = new AtomicBoolean(false);

    // Writer thread - repeatedly changes file size
    var writerThread = new Thread(() -> {
      try {
        for (int i = 0; i < 50; i++) {
          var size = 100 + (i * 50);
          var data = new byte[size];
          Arrays.fill(data, (byte) 'X');
          try (var channel = Files.newByteChannel(file, StandardOpenOption.WRITE)) {
            channel.position(0);
            channel.write(ByteBuffer.wrap(data));
          }
          Thread.sleep(5);
        }
      } catch (Exception e) {
        errors.set(true);
        e.printStackTrace();
      } finally {
        writerDone.set(true);
      }
    });

    // Attribute reader thread
    var readerThread = new Thread(() -> {
      try {
        while (!writerDone.get() && !errors.get()) {
          var attrs = Files.readAttributes(file, "*");
          assertNotNull(attrs.get("size"), "Size attribute should exist");
          var size = (Long) attrs.get("size");
          assertTrue(size >= 0, "Size should be non-negative");
          Thread.sleep(2);
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

    assertFalse(errors.get(), "No errors reading attributes during writes");
  }

  // ==================== Space Contention ====================

  /**
   * Verifies behavior when multiple threads create large files competing for space.
   */
  @Test
  void concurrentLargeFileCreation() throws Exception {
    int threadCount = 5;
    int fileSize = 4096 * 10; // 10 blocks each

    var errors = new AtomicInteger(0);
    var successCount = new AtomicInteger(0);
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int fileNum = t;
      new Thread(() -> {
        try {
          startLatch.await();
          var file = fs.getPath("/large" + fileNum + ".bin");
          var data = new byte[fileSize];
          Arrays.fill(data, (byte) ('A' + fileNum));
          Files.write(file, data);
          successCount.incrementAndGet();
        } catch (IOException e) {
          // Space exhaustion is acceptable
          if (!e.getMessage().contains("space")) {
            errors.incrementAndGet();
            e.printStackTrace();
          }
        } catch (Exception e) {
          errors.incrementAndGet();
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete");

    assertEquals(0, errors.get(), "No unexpected errors");
    assertTrue(successCount.get() > 0, "At least some files should be created");
  }

  /**
   * Verifies behavior when multiple threads extend files simultaneously.
   */
  @Test
  void concurrentFileExtension() throws Exception {
    int threadCount = 5;

    // Create initial small files
    for (int i = 0; i < threadCount; i++) {
      Files.write(fs.getPath("/extend" + i + ".bin"), new byte[100]);
    }

    var errors = new AtomicInteger(0);
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      int fileNum = t;
      new Thread(() -> {
        try {
          startLatch.await();
          var file = fs.getPath("/extend" + fileNum + ".bin");
          // Extend file by appending
          for (int i = 0; i < 10; i++) {
            try (var channel = Files.newByteChannel(file,
              StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
              var data = new byte[100];
              Arrays.fill(data, (byte) ('A' + fileNum));
              channel.write(ByteBuffer.wrap(data));
            }
          }
        } catch (IOException e) {
          // Space exhaustion is acceptable
          if (!e.getMessage().contains("space")) {
            errors.incrementAndGet();
            e.printStackTrace();
          }
        } catch (Exception e) {
          errors.incrementAndGet();
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete");
    assertEquals(0, errors.get(), "No unexpected errors during concurrent extension");
  }

  // ==================== Filesystem Close ====================

  /**
   * Verifies behavior when operations are in progress during filesystem close.
   */
  @Test
  void operationsDuringFilesystemClose() throws Exception {
    // Create a separate filesystem for this test
    var testContainer = tempDir.resolve("close-test.box");
    var testUri = URI.create("box:" + testContainer);

    try (var testFs = FileSystems.newFileSystem(testUri, Map.of("create", "true", "totalBlocks", 256L))) {
      var file = testFs.getPath("/close-test.bin");
      Files.write(file, new byte[1000]);

      var operationStarted = new CountDownLatch(1);
      var errors = new AtomicInteger(0);

      // Operation thread - starts a long read
      var operationThread = new Thread(() -> {
        try {
          operationStarted.countDown();
          // Perform operation
          Files.readAllBytes(file);
        } catch (IOException e) {
          // Expected if filesystem was closed
        } catch (Exception e) {
          errors.incrementAndGet();
          e.printStackTrace();
        }
      });

      operationThread.start();
      operationStarted.await();
      Thread.sleep(10);

      // Close filesystem while operation might be in progress
      testFs.close();

      operationThread.join(5000);

      assertEquals(0, errors.get(), "No unexpected errors");
      // Operation either completed successfully or failed due to close - both are acceptable
    }
  }

  /**
   * Verifies behavior when multiple threads try to close the filesystem.
   */
  @Test
  void multipleThreadsClosingFilesystem() throws Exception {
    // Create a separate filesystem for this test
    var testContainer = tempDir.resolve("multi-close.box");
    var testUri = URI.create("box:" + testContainer);

    try (var testFs = FileSystems.newFileSystem(testUri, Map.of("create", "true", "totalBlocks", 256L))) {
      int threadCount = 5;
      var errors = new AtomicInteger(0);
      var startLatch = new CountDownLatch(1);
      var doneLatch = new CountDownLatch(threadCount);

      for (int t = 0; t < threadCount; t++) {
        new Thread(() -> {
          try {
            startLatch.await();
            testFs.close(); // Multiple threads calling close
          } catch (Exception e) {
            // Close might throw if already closed - check if it's expected
            if (!(e instanceof IllegalStateException ||
              e.getMessage().contains("closed"))) {
              errors.incrementAndGet();
              e.printStackTrace();
            }
          } finally {
            doneLatch.countDown();
          }
        }).start();
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete");
      assertEquals(0, errors.get(), "No unexpected errors during concurrent close");
      assertFalse(testFs.isOpen(), "Filesystem should be closed");
    }
  }

  // ==================== Path Operations ====================

  /**
   * Verifies behavior when creating nested directories concurrently.
   */
  @Test
  void createNestedDirectoriesConcurrently() throws Exception {
    int threadCount = 5;
    var errors = new AtomicInteger(0);
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);

    // All threads try to create overlapping directory structures
    for (int t = 0; t < threadCount; t++) {
      int threadNum = t;
      new Thread(() -> {
        try {
          startLatch.await();
          // Create /a/b/c{threadNum}
          var path = fs.getPath("/nested/shared/thread" + threadNum);
          Files.createDirectories(path);
          assertTrue(Files.isDirectory(path), "Directory should exist");
        } catch (Exception e) {
          errors.incrementAndGet();
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete");
    assertEquals(0, errors.get(), "No errors during concurrent directory creation");

    // Verify structure
    assertTrue(Files.isDirectory(fs.getPath("/nested")));
    assertTrue(Files.isDirectory(fs.getPath("/nested/shared")));
    for (int t = 0; t < threadCount; t++) {
      assertTrue(Files.isDirectory(fs.getPath("/nested/shared/thread" + t)));
    }
  }

  /**
   * Verifies behavior when deleting a parent directory while child is being accessed.
   */
  @Test
  void deleteParentWhileAccessingChild() throws Exception {
    var parent = fs.getPath("/parent-delete");
    var child = fs.getPath("/parent-delete/child.txt");
    Files.createDirectory(parent);
    Files.write(child, "child content".getBytes());

    var childAccessStarted = new CountDownLatch(1);
    var deleteAttempted = new AtomicBoolean(false);
    var errors = new AtomicInteger(0);

    // Child accessor thread
    var accessorThread = new Thread(() -> {
      try (var channel = Files.newByteChannel(child, StandardOpenOption.READ)) {
        childAccessStarted.countDown();
        Thread.sleep(100);
        try {
          var buffer = ByteBuffer.allocate(100);
          channel.read(buffer);
        } catch (IOException e) {
          // Expected if parent was deleted
        }
      } catch (Exception e) {
        errors.incrementAndGet();
        e.printStackTrace();
      }
    });

    // Parent deleter thread
    var deleterThread = new Thread(() -> {
      try {
        childAccessStarted.await();
        Thread.sleep(20);
        try {
          // Try to delete parent (should fail because child exists)
          Files.delete(parent);
        } catch (IOException e) {
          // Expected - directory not empty
        }
        deleteAttempted.set(true);
      } catch (Exception e) {
        errors.incrementAndGet();
        e.printStackTrace();
      }
    });

    accessorThread.start();
    deleterThread.start();

    accessorThread.join(5000);
    deleterThread.join(5000);

    assertTrue(deleteAttempted.get(), "Delete should have been attempted");
    assertEquals(0, errors.get(), "No unexpected errors");
  }

  /**
   * Verifies concurrent file existence checks don't cause issues.
   */
  @Test
  void concurrentExistenceChecks() throws Exception {
    var file = fs.getPath("/existence-check.txt");

    int threadCount = 10;
    var errors = new AtomicBoolean(false);
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);

    // Half threads check existence, half create/delete
    for (int t = 0; t < threadCount; t++) {
      boolean isChecker = t % 2 == 0;
      new Thread(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 50; i++) {
            if (isChecker) {
              // Just check existence
              Files.exists(file);
            } else {
              // Toggle file existence
              if (Files.exists(file)) {
                try {
                  Files.delete(file);
                } catch (IOException e) {
                  // Another thread might have deleted it
                }
              } else {
                try {
                  Files.write(file, "x".getBytes());
                } catch (IOException e) {
                  // Another thread might have created it
                }
              }
            }
          }
        } catch (Exception e) {
          errors.set(true);
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete");
    assertFalse(errors.get(), "No errors during concurrent existence checks");
  }
}
