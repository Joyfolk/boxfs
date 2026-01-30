package org.test.boxfs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the BoxFs facade API.
 */
class BoxFsTest {

  @TempDir
  Path tempDir;

  @Test
  void createAndOpenContainer() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/test.txt")) {
        out.write("hello".getBytes());
      }
    }

    try (var fs = BoxFs.open(containerPath)) {
      try (var in = fs.openRead("/test.txt")) {
        assertArrayEquals("hello".getBytes(), in.readAllBytes());
      }
    }
  }

  @Test
  void writeAndReadFile() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/document.txt")) {
        out.write("Hello, World!".getBytes());
      }

      try (var in = fs.openRead("/document.txt")) {
        assertEquals("Hello, World!", new String(in.readAllBytes()));
      }
    }
  }

  @Test
  void appendToFile() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/log.txt")) {
        out.write("Line 1\n".getBytes());
      }
      try (var out = fs.openAppend("/log.txt")) {
        out.write("Line 2\n".getBytes());
      }

      try (var in = fs.openRead("/log.txt")) {
        assertEquals("Line 1\nLine 2\n", new String(in.readAllBytes()));
      }
    }
  }

  @Test
  void deleteFile() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/temp.txt")) {
        out.write("temporary".getBytes());
      }
      assertTrue(fs.exists("/temp.txt"));

      fs.deleteFile("/temp.txt");
      assertFalse(fs.exists("/temp.txt"));
    }
  }

  @Test
  void renameFile() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/old.txt")) {
        out.write("content".getBytes());
      }

      fs.renameFile("/old.txt", "/new.txt");

      assertFalse(fs.exists("/old.txt"));
      assertTrue(fs.exists("/new.txt"));
      try (var in = fs.openRead("/new.txt")) {
        assertEquals("content", new String(in.readAllBytes()));
      }
    }
  }

  @Test
  void moveFile() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      fs.createDirectory("/source");
      fs.createDirectory("/target");
      try (var out = fs.openWrite("/source/file.txt")) {
        out.write("data".getBytes());
      }

      fs.moveFile("/source/file.txt", "/target/file.txt");

      assertFalse(fs.exists("/source/file.txt"));
      assertTrue(fs.exists("/target/file.txt"));
      try (var in = fs.openRead("/target/file.txt")) {
        assertEquals("data", new String(in.readAllBytes()));
      }
    }
  }

  @Test
  void createAndListDirectory() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      fs.createDirectory("/mydir");
      try (var out = fs.openWrite("/mydir/a.txt")) {
        out.write("a".getBytes());
      }
      try (var out = fs.openWrite("/mydir/b.txt")) {
        out.write("b".getBytes());
      }
      fs.createDirectory("/mydir/subdir");

      var entries = fs.listDirectory("/mydir");
      assertEquals(3, entries.size());
      assertTrue(entries.containsAll(List.of("a.txt", "b.txt", "subdir")));
    }
  }

  @Test
  void createDirectoriesNested() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      fs.createDirectories("/a/b/c/d");

      assertTrue(fs.isDirectory("/a"));
      assertTrue(fs.isDirectory("/a/b"));
      assertTrue(fs.isDirectory("/a/b/c"));
      assertTrue(fs.isDirectory("/a/b/c/d"));
    }
  }

  @Test
  void deleteDirectory() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      fs.createDirectory("/emptydir");
      assertTrue(fs.exists("/emptydir"));

      fs.deleteDirectory("/emptydir");
      assertFalse(fs.exists("/emptydir"));
    }
  }

  @Test
  void isFileAndIsDirectory() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      fs.createDirectory("/dir");
      try (var out = fs.openWrite("/file.txt")) {
        out.write("content".getBytes());
      }

      assertTrue(fs.isDirectory("/dir"));
      assertFalse(fs.isFile("/dir"));

      assertTrue(fs.isFile("/file.txt"));
      assertFalse(fs.isDirectory("/file.txt"));
    }
  }

  @Test
  void fileSize() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/sized.bin")) {
        out.write(new byte[1234]);
      }

      assertEquals(1234, fs.size("/sized.bin"));
    }
  }

  @Test
  void syncPersistsData() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      try (var out = fs.openWrite("/synced.txt")) {
        out.write("important data".getBytes());
      }
      fs.sync();
    }

    try (var fs = BoxFs.open(containerPath)) {
      try (var in = fs.openRead("/synced.txt")) {
        assertEquals("important data", new String(in.readAllBytes()));
      }
    }
  }

  @Test
  void getUnderlyingFileSystem() throws IOException {
    var containerPath = tempDir.resolve("test.box");

    try (var fs = BoxFs.create(containerPath)) {
      var nioFs = fs.getFileSystem();
      assertNotNull(nioFs);
      assertInstanceOf(BoxFileSystem.class, nioFs);
    }
  }

  @Test
  void createWithCustomCapacity() throws IOException {
    var containerPath = tempDir.resolve("large.box");

    try (var fs = BoxFs.create(containerPath, 1024)) {
      try (var out = fs.openWrite("/large.bin")) {
        out.write(new byte[100_000]);
      }
      assertEquals(100_000, fs.size("/large.bin"));
    }
  }

  @Test
  void completeWorkflow() throws IOException {
    var containerPath = tempDir.resolve("workflow.box");

    try (var fs = BoxFs.create(containerPath)) {
      // Create structure
      fs.createDirectories("/project/src");
      fs.createDirectories("/project/docs");

      // Write files
      try (var out = fs.openWrite("/project/README.md")) {
        out.write("# My Project".getBytes());
      }
      try (var out = fs.openWrite("/project/src/Main.java")) {
        out.write("class Main {}".getBytes());
      }
      try (var out = fs.openWrite("/project/docs/guide.txt")) {
        out.write("User guide".getBytes());
      }

      // Read and verify
      try (var in = fs.openRead("/project/README.md")) {
        assertTrue(new String(in.readAllBytes()).contains("My Project"));
      }

      // Append
      try (var out = fs.openAppend("/project/README.md")) {
        out.write("\n\nMore content".getBytes());
      }
      try (var in = fs.openRead("/project/README.md")) {
        assertTrue(new String(in.readAllBytes()).contains("More content"));
      }

      // Rename
      fs.renameFile("/project/docs/guide.txt", "/project/docs/manual.txt");
      assertFalse(fs.exists("/project/docs/guide.txt"));
      assertTrue(fs.exists("/project/docs/manual.txt"));

      // Move
      fs.moveFile("/project/docs/manual.txt", "/project/manual.txt");
      assertTrue(fs.exists("/project/manual.txt"));

      // Delete
      fs.deleteFile("/project/manual.txt");
      assertFalse(fs.exists("/project/manual.txt"));

      // List
      var srcFiles = fs.listDirectory("/project/src");
      assertEquals(1, srcFiles.size());
      assertEquals("Main.java", srcFiles.getFirst());
    }

    // Verify persistence
    try (var fs = BoxFs.open(containerPath)) {
      assertTrue(fs.exists("/project/README.md"));
      assertTrue(fs.exists("/project/src/Main.java"));
    }
  }
}
