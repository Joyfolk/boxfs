package org.test.boxfs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests using standard Files API.
 */
class BoxFsIntegrationTest {

    @TempDir
    Path tempDir;

    private FileSystem fs;
    private Path container;

    @BeforeEach
    void setUp() throws IOException {
        container = tempDir.resolve("test.box");
        var uri = URI.create("box:" + container);
        fs = FileSystems.newFileSystem(uri, Map.of("create", "true", "totalBlocks", 256L));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fs != null && fs.isOpen()) {
            fs.close();
        }
    }

    @Test
    void createAndReadFile() throws IOException {
        var file = fs.getPath("/test.txt");
        var content = "Hello, BoxFS!".getBytes(StandardCharsets.UTF_8);

        Files.write(file, content);

        assertTrue(Files.exists(file));
        assertArrayEquals(content, Files.readAllBytes(file));
    }

    @Test
    void createDirectory() throws IOException {
        var dir = fs.getPath("/mydir");
        Files.createDirectory(dir);

        assertTrue(Files.exists(dir));
        assertTrue(Files.isDirectory(dir));
    }

    @Test
    void createDirectories() throws IOException {
        var dir = fs.getPath("/a/b/c");
        Files.createDirectories(dir);

        assertTrue(Files.exists(dir));
        assertTrue(Files.exists(fs.getPath("/a")));
        assertTrue(Files.exists(fs.getPath("/a/b")));
    }

    @Test
    void deleteFile() throws IOException {
        var file = fs.getPath("/todelete.txt");
        Files.write(file, "delete me".getBytes());

        assertTrue(Files.exists(file));

        Files.delete(file);

        assertFalse(Files.exists(file));
    }

    @Test
    void deleteEmptyDirectory() throws IOException {
        var dir = fs.getPath("/emptydir");
        Files.createDirectory(dir);
        Files.delete(dir);

        assertFalse(Files.exists(dir));
    }

    @Test
    void deleteNonEmptyDirectoryFails() throws IOException {
        var dir = fs.getPath("/nonempty");
        Files.createDirectory(dir);
        Files.write(dir.resolve("file.txt"), "content".getBytes());

        assertThrows(DirectoryNotEmptyException.class, () -> Files.delete(dir));
    }

    @Test
    void renameFile() throws IOException {
        var source = fs.getPath("/source.txt");
        var target = fs.getPath("/target.txt");
        Files.write(source, "content".getBytes());

        Files.move(source, target);

        assertFalse(Files.exists(source));
        assertTrue(Files.exists(target));
        assertEquals("content", Files.readString(target));
    }

    @Test
    void moveFileBetweenDirectories() throws IOException {
        var dir1 = fs.getPath("/dir1");
        var dir2 = fs.getPath("/dir2");
        Files.createDirectory(dir1);
        Files.createDirectory(dir2);

        var source = dir1.resolve("file.txt");
        var target = dir2.resolve("moved.txt");
        Files.write(source, "moving".getBytes());

        Files.move(source, target);

        assertFalse(Files.exists(source));
        assertTrue(Files.exists(target));
        assertEquals("moving", Files.readString(target));
    }

    @Test
    void moveFromBoxFsToOsFileSystem() throws IOException {
        var boxFile = fs.getPath("/tomove.txt");
        var osFile = tempDir.resolve("moved.txt");
        Files.write(boxFile, "move me".getBytes());

        Files.move(boxFile, osFile);

        assertFalse(Files.exists(boxFile));
        assertTrue(Files.exists(osFile));
        assertEquals("move me", Files.readString(osFile));
    }

    @Test
    void moveFromOsFileSystemToBoxFs() throws IOException {
        var osFile = tempDir.resolve("tomove.txt");
        var boxFile = fs.getPath("/moved.txt");
        Files.write(osFile, "move me".getBytes());

        Files.move(osFile, boxFile);

        assertFalse(Files.exists(osFile));
        assertTrue(Files.exists(boxFile));
        assertEquals("move me", Files.readString(boxFile));
    }

    @Test
    void moveBetweenDifferentContainers() throws IOException {
        var container2 = tempDir.resolve("test2.box");
        var uri2 = URI.create("box:" + container2);

        try (var fs2 = FileSystems.newFileSystem(uri2, Map.of("create", "true", "totalBlocks", 64L))) {
            var source = fs.getPath("/source.txt");
            var target = fs2.getPath("/target.txt");
            Files.write(source, "content".getBytes());

            Files.move(source, target);

            assertFalse(Files.exists(source));
            assertTrue(Files.exists(target));
            assertEquals("content", Files.readString(target));
        }
    }

    @Test
    void copyFile() throws IOException {
        var source = fs.getPath("/source.txt");
        var target = fs.getPath("/copy.txt");
        Files.write(source, "copy me".getBytes());

        Files.copy(source, target);

        assertTrue(Files.exists(source));
        assertTrue(Files.exists(target));
        assertEquals("copy me", Files.readString(source));
        assertEquals("copy me", Files.readString(target));
    }

    @Test
    void copyFileBetweenDirectories() throws IOException {
        var dir1 = fs.getPath("/dir1");
        var dir2 = fs.getPath("/dir2");
        Files.createDirectory(dir1);
        Files.createDirectory(dir2);

        var source = dir1.resolve("file.txt");
        var target = dir2.resolve("copied.txt");
        Files.write(source, "copying".getBytes());

        Files.copy(source, target);

        assertTrue(Files.exists(source));
        assertTrue(Files.exists(target));
        assertEquals("copying", Files.readString(source));
        assertEquals("copying", Files.readString(target));
    }

    @Test
    void copyFileReplaceExisting() throws IOException {
        var source = fs.getPath("/source.txt");
        var target = fs.getPath("/target.txt");
        Files.write(source, "new content".getBytes());
        Files.write(target, "old content".getBytes());

        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

        assertEquals("new content", Files.readString(target));
    }

    @Test
    void copyFileFailsIfTargetExists() throws IOException {
        var source = fs.getPath("/source.txt");
        var target = fs.getPath("/target.txt");
        Files.write(source, "source".getBytes());
        Files.write(target, "target".getBytes());

        assertThrows(FileAlreadyExistsException.class, () -> Files.copy(source, target));
    }

    @Test
    void copyLargeFile() throws IOException {
        var source = fs.getPath("/large.bin");
        var target = fs.getPath("/large_copy.bin");
        var data = new byte[50_000];
        for (var i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        Files.write(source, data);

        Files.copy(source, target);

        assertArrayEquals(data, Files.readAllBytes(target));
    }

    @Test
    void copyEmptyFile() throws IOException {
        var source = fs.getPath("/empty.txt");
        var target = fs.getPath("/empty_copy.txt");
        Files.write(source, new byte[0]);

        Files.copy(source, target);

        assertTrue(Files.exists(target));
        assertEquals(0, Files.size(target));
    }

    @Test
    void copyNonExistentFileFails() {
        var source = fs.getPath("/nonexistent.txt");
        var target = fs.getPath("/target.txt");

        assertThrows(NoSuchFileException.class, () -> Files.copy(source, target));
    }

    @Test
    void copyFromBoxFsToOsFileSystem() throws IOException {
        var boxFile = fs.getPath("/export.txt");
        var osFile = tempDir.resolve("exported.txt");
        Files.write(boxFile, "export me".getBytes());

        Files.copy(boxFile, osFile);

        assertTrue(Files.exists(osFile));
        assertEquals("export me", Files.readString(osFile));
    }

    @Test
    void copyFromOsFileSystemToBoxFs() throws IOException {
        var osFile = tempDir.resolve("import.txt");
        var boxFile = fs.getPath("/imported.txt");
        Files.write(osFile, "import me".getBytes());

        Files.copy(osFile, boxFile);

        assertTrue(Files.exists(boxFile));
        assertEquals("import me", Files.readString(boxFile));
    }

    @Test
    void copyBetweenDifferentContainers() throws IOException {
        var container2 = tempDir.resolve("test2.box");
        var uri2 = URI.create("box:" + container2);

        try (var fs2 = FileSystems.newFileSystem(uri2, Map.of("create", "true", "totalBlocks", 64L))) {
            var source = fs.getPath("/source.txt");
            var target = fs2.getPath("/target.txt");
            Files.write(source, "content".getBytes());

            Files.copy(source, target);

            assertTrue(Files.exists(source));
            assertTrue(Files.exists(target));
            assertEquals("content", Files.readString(source));
            assertEquals("content", Files.readString(target));
        }
    }

    @Test
    void copyBetweenDifferentContainersReplaceExisting() throws IOException {
        var container2 = tempDir.resolve("test2.box");
        var uri2 = URI.create("box:" + container2);

        try (var fs2 = FileSystems.newFileSystem(uri2, Map.of("create", "true", "totalBlocks", 64L))) {
            var source = fs.getPath("/source.txt");
            var target = fs2.getPath("/target.txt");
            Files.write(source, "new content".getBytes());
            Files.write(target, "old content".getBytes());

            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

            assertEquals("new content", Files.readString(target));
        }
    }

    @Test
    void copyBetweenDifferentContainersFailsIfTargetExists() throws IOException {
        var container2 = tempDir.resolve("test2.box");
        var uri2 = URI.create("box:" + container2);

        try (var fs2 = FileSystems.newFileSystem(uri2, Map.of("create", "true", "totalBlocks", 64L))) {
            var source = fs.getPath("/source.txt");
            var target = fs2.getPath("/target.txt");
            Files.write(source, "source".getBytes());
            Files.write(target, "target".getBytes());

            assertThrows(FileAlreadyExistsException.class, () -> Files.copy(source, target));
        }
    }

    // POSIX semantics: cannot replace directory with file or vice versa
    @Test
    void moveFileToReplaceDirectoryFails() throws IOException {
        var file = fs.getPath("/source.txt");
        var dir = fs.getPath("/target");
        Files.write(file, "content".getBytes());
        Files.createDirectory(dir);

        var ex = assertThrows(IOException.class,
                () -> Files.move(file, dir, StandardCopyOption.REPLACE_EXISTING));
        assertTrue(ex.getMessage().contains("Cannot replace directory with file"));
    }

    @Test
    void moveDirectoryToReplaceFileFails() throws IOException {
        var dir = fs.getPath("/source");
        var file = fs.getPath("/target.txt");
        Files.createDirectory(dir);
        Files.write(file, "content".getBytes());

        var ex = assertThrows(IOException.class,
                () -> Files.move(dir, file, StandardCopyOption.REPLACE_EXISTING));
        assertTrue(ex.getMessage().contains("Cannot replace file with directory"));
    }

    @Test
    void copyFileToReplaceDirectoryFails() throws IOException {
        var file = fs.getPath("/source.txt");
        var dir = fs.getPath("/target");
        Files.write(file, "content".getBytes());
        Files.createDirectory(dir);

        var ex = assertThrows(IOException.class,
                () -> Files.copy(file, dir, StandardCopyOption.REPLACE_EXISTING));
        assertTrue(ex.getMessage().contains("Cannot replace directory with file"));
    }

    @Test
    void listDirectory() throws IOException {
        var dir = fs.getPath("/listtest");
        Files.createDirectory(dir);
        Files.write(dir.resolve("a.txt"), "a".getBytes());
        Files.write(dir.resolve("b.txt"), "b".getBytes());
        Files.createDirectory(dir.resolve("subdir"));

        var entries = new ArrayList<Path>();
        try (var stream = Files.newDirectoryStream(dir)) {
            for (var p : stream) {
                entries.add(p);
            }
        }

        assertEquals(3, entries.size());
    }

    @Test
    void fileAttributes() throws IOException {
        var file = fs.getPath("/attrs.txt");
        Files.write(file, "0123456789".getBytes());

        var attrs = Files.readAttributes(file, BasicFileAttributes.class);

        assertTrue(attrs.isRegularFile());
        assertFalse(attrs.isDirectory());
        assertEquals(10, attrs.size());
    }

    @Test
    void directoryAttributes() throws IOException {
        var dir = fs.getPath("/attrdir");
        Files.createDirectory(dir);

        var attrs = Files.readAttributes(dir, BasicFileAttributes.class);

        assertFalse(attrs.isRegularFile());
        assertTrue(attrs.isDirectory());
    }

    @Test
    void fileTimestamps() throws IOException {
        var beforeCreate = System.currentTimeMillis();
        var file = fs.getPath("/timestamps.txt");
        Files.write(file, "initial".getBytes());
        var afterCreate = System.currentTimeMillis();

        var attrs = Files.readAttributes(file, BasicFileAttributes.class);

        // Creation time should be set
        var creationTime = attrs.creationTime().toMillis();
        assertTrue(creationTime >= beforeCreate && creationTime <= afterCreate,
                "Creation time should be between beforeCreate and afterCreate");

        // Modification time should match creation time initially
        assertEquals(attrs.lastModifiedTime().toMillis(), attrs.lastAccessTime().toMillis());

        // Wait a bit and modify the file
        try { Thread.sleep(10); } catch (InterruptedException ignored) {}
        var beforeModify = System.currentTimeMillis();
        Files.write(file, "modified".getBytes());
        var afterModify = System.currentTimeMillis();

        var attrsAfterModify = Files.readAttributes(file, BasicFileAttributes.class);
        var modifiedTime = attrsAfterModify.lastModifiedTime().toMillis();

        assertTrue(modifiedTime >= beforeModify && modifiedTime <= afterModify,
                "Modified time should be updated after write");
        assertEquals(creationTime, attrsAfterModify.creationTime().toMillis(),
                "Creation time should not change");
    }

    @Test
    void setFileTimestamps() throws IOException {
        var file = fs.getPath("/settime.txt");
        Files.write(file, "content".getBytes());

        var customTime = java.nio.file.attribute.FileTime.fromMillis(1000000000000L);
        var view = Files.getFileAttributeView(file, java.nio.file.attribute.BasicFileAttributeView.class);
        view.setTimes(customTime, null, null);

        var attrs = Files.readAttributes(file, BasicFileAttributes.class);
        assertEquals(1000000000000L, attrs.lastModifiedTime().toMillis());
    }

    @Test
    void appendToFile() throws IOException {
        var file = fs.getPath("/append.txt");
        Files.write(file, "Hello".getBytes());
        Files.write(file, " World".getBytes(), StandardOpenOption.APPEND);

        assertEquals("Hello World", Files.readString(file));
    }

    @Test
    void overwriteFile() throws IOException {
        var file = fs.getPath("/overwrite.txt");
        Files.write(file, "original".getBytes());
        Files.write(file, "new".getBytes());

        assertEquals("new", Files.readString(file));
    }

    @Test
    void largeFile() throws IOException {
        var file = fs.getPath("/large.bin");
        var data = new byte[100_000]; // 100KB
        for (var i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        Files.write(file, data);
        var read = Files.readAllBytes(file);

        assertArrayEquals(data, read);
    }

    @Test
    void seekableByteChannel() throws IOException {
        var file = fs.getPath("/seekable.bin");
        var data = "0123456789".getBytes();

        // Write
        try (var channel = Files.newByteChannel(file,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(java.nio.ByteBuffer.wrap(data));
        }

        // Read with seeking
        try (var channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
            channel.position(5);
            var buf = java.nio.ByteBuffer.allocate(5);
            channel.read(buf);
            assertEquals("56789", new String(buf.array()));
        }
    }

    @Test
    void truncateFile() throws IOException {
        var file = fs.getPath("/truncate.txt");
        Files.write(file, "0123456789".getBytes());

        try (var channel = Files.newByteChannel(file,
                StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            channel.truncate(5);
            assertEquals(5, channel.size());
        }

        assertEquals("01234", Files.readString(file));
    }

    @Test
    void fileStoreInfo() throws IOException {
        var store = fs.getFileStores().iterator().next();

        assertTrue(store.getTotalSpace() > 0);
        assertTrue(store.getUsableSpace() >= 0);
        assertEquals("boxfs", store.type());
    }

    @Test
    void pathMatcher() {
        var matcher = fs.getPathMatcher("glob:*.txt");
        assertTrue(matcher.matches(fs.getPath("file.txt")));
        assertFalse(matcher.matches(fs.getPath("file.bin")));
    }

    @Test
    void reopenContainer() throws IOException {
        // Write file and close
        var file = fs.getPath("/persist.txt");
        Files.write(file, "persisted".getBytes());
        fs.close();

        // Reopen and verify
        var uri = URI.create("box:" + container);
        fs = FileSystems.newFileSystem(uri, Map.of());

        assertTrue(Files.exists(fs.getPath("/persist.txt")));
        assertEquals("persisted", Files.readString(fs.getPath("/persist.txt")));
    }

    @Test
    void metadataGrowthWithFragmentation() throws IOException {
        // Create a small filesystem to make fragmentation more likely
        var smallContainer = tempDir.resolve("small.box");
        var smallUri = URI.create("box:" + smallContainer);

        try (var smallFs = FileSystems.newFileSystem(smallUri,
                Map.of("create", "true", "totalBlocks", 32L, "blockSize", 512))) {

            // Create many files to grow metadata and potentially fragment
            for (int i = 0; i < 20; i++) {
                var file = smallFs.getPath("/file" + i + ".txt");
                Files.write(file, ("content" + i).getBytes());
            }

            // Delete some files to create gaps in free space
            for (int i = 0; i < 20; i += 2) {
                Files.delete(smallFs.getPath("/file" + i + ".txt"));
            }

            // Create more files - metadata grows while free space is fragmented
            for (int i = 20; i < 30; i++) {
                var file = smallFs.getPath("/file" + i + ".txt");
                Files.write(file, ("content" + i).getBytes());
            }
        }

        // Reopen and verify everything is intact
        try (var smallFs = FileSystems.newFileSystem(smallUri, Map.of())) {
            // Odd files should exist
            for (int i = 1; i < 20; i += 2) {
                assertTrue(Files.exists(smallFs.getPath("/file" + i + ".txt")));
            }
            // New files should exist
            for (int i = 20; i < 30; i++) {
                assertTrue(Files.exists(smallFs.getPath("/file" + i + ".txt")));
            }
            // Even files should be deleted
            for (int i = 0; i < 20; i += 2) {
                assertFalse(Files.exists(smallFs.getPath("/file" + i + ".txt")));
            }
        }
    }
}
