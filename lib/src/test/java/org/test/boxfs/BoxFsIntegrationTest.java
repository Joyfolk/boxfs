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
import java.util.List;
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
}
