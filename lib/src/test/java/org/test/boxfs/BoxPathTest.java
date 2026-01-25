package org.test.boxfs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BoxPathTest {

    @TempDir
    Path tempDir;

    private FileSystem fs;

    @BeforeEach
    void setUp() throws IOException {
        var container = tempDir.resolve("test.box");
        var uri = URI.create("box:" + container);
        fs = FileSystems.newFileSystem(uri, Map.of("create", "true", "totalBlocks", 64L));
    }

    @Test
    void absolutePath() {
        var path = fs.getPath("/foo/bar");
        assertTrue(path.isAbsolute());
        assertEquals("/foo/bar", path.toString());
    }

    @Test
    void relativePath() {
        var path = fs.getPath("foo/bar");
        assertFalse(path.isAbsolute());
        assertEquals("foo/bar", path.toString());
    }

    @Test
    void rootPath() {
        var path = fs.getPath("/");
        assertTrue(path.isAbsolute());
        assertEquals("/", path.toString());
        assertEquals(0, path.getNameCount());
    }

    @Test
    void getFileName() {
        var path = fs.getPath("/foo/bar/baz.txt");
        assertEquals("baz.txt", path.getFileName().toString());
    }

    @Test
    void getParent() {
        var path = fs.getPath("/foo/bar/baz.txt");
        assertEquals("/foo/bar", path.getParent().toString());
    }

    @Test
    void getNameCount() {
        var path = fs.getPath("/foo/bar/baz");
        assertEquals(3, path.getNameCount());
    }

    @Test
    void getName() {
        var path = fs.getPath("/foo/bar/baz");
        assertEquals("foo", path.getName(0).toString());
        assertEquals("bar", path.getName(1).toString());
        assertEquals("baz", path.getName(2).toString());
    }

    @Test
    void resolve() {
        var base = fs.getPath("/foo");
        var resolved = base.resolve("bar/baz");
        assertEquals("/foo/bar/baz", resolved.toString());
    }

    @Test
    void resolveAbsolute() {
        var base = fs.getPath("/foo");
        var resolved = base.resolve("/bar");
        assertEquals("/bar", resolved.toString());
    }

    @Test
    void relativize() {
        var base = fs.getPath("/foo/bar");
        var target = fs.getPath("/foo/bar/baz/qux");
        var relative = base.relativize(target);
        assertEquals("baz/qux", relative.toString());
    }

    @Test
    void normalize() {
        var path = fs.getPath("/foo/bar/../baz/./qux");
        assertEquals("/foo/baz/qux", path.normalize().toString());
    }

    @Test
    void startsWith() {
        var path = fs.getPath("/foo/bar/baz");
        assertTrue(path.startsWith(fs.getPath("/foo")));
        assertTrue(path.startsWith(fs.getPath("/foo/bar")));
        assertFalse(path.startsWith(fs.getPath("/bar")));
    }

    @Test
    void endsWith() {
        var path = fs.getPath("/foo/bar/baz");
        assertTrue(path.endsWith(fs.getPath("baz")));
        assertTrue(path.endsWith(fs.getPath("bar/baz")));
        assertFalse(path.endsWith(fs.getPath("foo")));
    }

    @Test
    void iterator() {
        var path = fs.getPath("/foo/bar/baz");
        var iter = path.iterator();
        assertEquals("foo", iter.next().toString());
        assertEquals("bar", iter.next().toString());
        assertEquals("baz", iter.next().toString());
        assertFalse(iter.hasNext());
    }

    @Test
    void toAbsolutePath() {
        var relative = fs.getPath("foo/bar");
        var absolute = relative.toAbsolutePath();
        assertTrue(absolute.isAbsolute());
        assertEquals("/foo/bar", absolute.toString());
    }

    @Test
    void compareTo() {
        var a = fs.getPath("/aaa");
        var b = fs.getPath("/bbb");
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(0, a.compareTo(fs.getPath("/aaa")));
    }

    @Test
    void equalsAndHashCode() {
        var a = fs.getPath("/foo/bar");
        var b = fs.getPath("/foo/bar");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
