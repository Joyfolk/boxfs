package org.test.boxfs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional test: Store project files, delete 70%, restore to new subfolder, verify content.
 */
class BoxFsFunctionalTest {

    @TempDir
    Path tempDir;

    private FileSystem fs;
    private Path container;

    @BeforeEach
    void setUp() throws IOException {
        container = tempDir.resolve("functional.box");
        var uri = URI.create("box:" + container);
        // Create a larger container for this test
        fs = FileSystems.newFileSystem(uri, Map.of("create", "true", "totalBlocks", 1024L));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fs != null && fs.isOpen()) {
            fs.close();
        }
    }

    @Test
    void storeDeleteRestoreVerify() throws IOException {
        // Step 1: Create test files with various content
        var originalFiles = createTestFiles();

        // Store all files in BoxFS
        var storageDir = fs.getPath("/storage");
        Files.createDirectory(storageDir);

        for (var entry : originalFiles.entrySet()) {
            var filePath = storageDir.resolve(entry.getKey());
            Files.createDirectories(filePath.getParent());
            Files.write(filePath, entry.getValue());
        }

        // Verify all files were stored
        var storedFiles = listAllFiles(storageDir);
        assertEquals(originalFiles.size(), storedFiles.size());

        // Step 2: Delete 70% of files
        var filesToDelete = selectFilesToDelete(storedFiles, 70);
        for (var fileName : filesToDelete) {
            var filePath = storageDir.resolve(fileName);
            Files.delete(filePath);
        }

        // Calculate remaining files
        var remainingFileNames = new HashSet<>(storedFiles);
        remainingFileNames.removeAll(filesToDelete);

        // Verify deletion
        var afterDelete = listAllFiles(storageDir);
        assertEquals(remainingFileNames.size(), afterDelete.size());

        // Step 3: Restore remaining files to new subfolder
        var restoreDir = fs.getPath("/restored");
        Files.createDirectory(restoreDir);

        for (var fileName : remainingFileNames) {
            var source = storageDir.resolve(fileName);
            var target = restoreDir.resolve(fileName);
            Files.createDirectories(target.getParent());

            // Read from source and write to target (copy)
            var content = Files.readAllBytes(source);
            Files.write(target, content);
        }

        // Step 4: Verify restored content matches original
        for (var fileName : remainingFileNames) {
            var restored = restoreDir.resolve(fileName);
            assertTrue(Files.exists(restored), "Restored file should exist: " + fileName);

            var restoredContent = Files.readAllBytes(restored);
            var originalContent = originalFiles.get(fileName);
            assertArrayEquals(originalContent, restoredContent,
                    "Content mismatch for file: " + fileName);
        }

        // Verify deleted files are not in restore
        for (var fileName : filesToDelete) {
            var restored = restoreDir.resolve(fileName);
            assertFalse(Files.exists(restored), "Deleted file should not be restored: " + fileName);
        }
    }

    @Test
    void completeWorkflow() throws IOException {
        // Simulates a complete user workflow with all operations

        // 1. Create directory structure
        Files.createDirectories(fs.getPath("/project/src/main"));
        Files.createDirectories(fs.getPath("/project/src/test"));
        Files.createDirectory(fs.getPath("/project/docs"));

        // 2. Write various files
        Files.write(fs.getPath("/project/README.md"), "# My Project\n\nDescription here.".getBytes());
        Files.write(fs.getPath("/project/src/main/App.java"),
                "public class App { public static void main(String[] args) {} }".getBytes());
        Files.write(fs.getPath("/project/src/test/AppTest.java"),
                "public class AppTest { @Test void test() {} }".getBytes());
        Files.write(fs.getPath("/project/docs/guide.txt"), "User guide content...".getBytes());

        // 3. Read and verify
        var readme = Files.readString(fs.getPath("/project/README.md"));
        assertTrue(readme.contains("My Project"));

        // 4. Append to file
        Files.write(fs.getPath("/project/README.md"), "\n## Features".getBytes(), StandardOpenOption.APPEND);
        readme = Files.readString(fs.getPath("/project/README.md"));
        assertTrue(readme.contains("Features"));

        // 5. Rename file
        Files.move(fs.getPath("/project/docs/guide.txt"), fs.getPath("/project/docs/manual.txt"));
        assertFalse(Files.exists(fs.getPath("/project/docs/guide.txt")));
        assertTrue(Files.exists(fs.getPath("/project/docs/manual.txt")));

        // 6. Move file between directories
        Files.move(fs.getPath("/project/docs/manual.txt"), fs.getPath("/project/manual.txt"));
        assertTrue(Files.exists(fs.getPath("/project/manual.txt")));

        // 7. Delete file
        Files.delete(fs.getPath("/project/manual.txt"));
        assertFalse(Files.exists(fs.getPath("/project/manual.txt")));

        // 8. List directory
        var srcContents = new ArrayList<Path>();
        try (var stream = Files.newDirectoryStream(fs.getPath("/project/src"))) {
            stream.forEach(srcContents::add);
        }
        assertEquals(2, srcContents.size()); // main and test

        // 9. Check attributes
        var attrs = Files.readAttributes(fs.getPath("/project/src/main/App.java"),
                java.nio.file.attribute.BasicFileAttributes.class);
        assertTrue(attrs.isRegularFile());
        assertTrue(attrs.size() > 0);

        // 10. Delete directory with content
        Files.delete(fs.getPath("/project/src/main/App.java"));
        Files.delete(fs.getPath("/project/src/main"));

        // 11. Verify final state
        assertTrue(Files.exists(fs.getPath("/project")));
        assertTrue(Files.exists(fs.getPath("/project/src")));
        assertTrue(Files.exists(fs.getPath("/project/src/test")));
        assertTrue(Files.exists(fs.getPath("/project/docs")));
        assertFalse(Files.exists(fs.getPath("/project/src/main")));
    }

    @Test
    void persistenceAcrossReopen() throws IOException {
        // Create content
        Files.createDirectories(fs.getPath("/data/nested/deep"));
        Files.write(fs.getPath("/data/file1.txt"), "Content 1".getBytes());
        Files.write(fs.getPath("/data/nested/file2.txt"), "Content 2".getBytes());
        Files.write(fs.getPath("/data/nested/deep/file3.txt"), "Content 3".getBytes());

        // Close the file system
        fs.close();

        // Reopen
        var uri = URI.create("box:" + container);
        fs = FileSystems.newFileSystem(uri, Map.of());

        // Verify everything persisted
        assertTrue(Files.exists(fs.getPath("/data")));
        assertTrue(Files.exists(fs.getPath("/data/nested")));
        assertTrue(Files.exists(fs.getPath("/data/nested/deep")));
        assertEquals("Content 1", Files.readString(fs.getPath("/data/file1.txt")));
        assertEquals("Content 2", Files.readString(fs.getPath("/data/nested/file2.txt")));
        assertEquals("Content 3", Files.readString(fs.getPath("/data/nested/deep/file3.txt")));
    }

    private Map<String, byte[]> createTestFiles() {
        var files = new LinkedHashMap<String, byte[]>();
        var random = new Random(42); // Fixed seed for reproducibility

        // Create various test files with different sizes
        files.put("readme.txt", "This is a readme file with project information.".getBytes());
        files.put("config.json", "{\"setting\": \"value\", \"enabled\": true}".getBytes());
        files.put("data/users.csv", "id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com".getBytes());
        files.put("data/products.csv", "id,name,price\n1,Widget,9.99\n2,Gadget,19.99".getBytes());
        files.put("src/main.java", "public class Main { public static void main(String[] args) { System.out.println(\"Hello\"); } }".getBytes());
        files.put("src/util/Helper.java", "public class Helper { public static void help() {} }".getBytes());
        files.put("src/util/Constants.java", "public class Constants { public static final int MAX = 100; }".getBytes());
        files.put("docs/api.md", "# API Documentation\n\n## Endpoints\n\n- GET /users\n- POST /users".getBytes());
        files.put("docs/guide.md", "# User Guide\n\n## Getting Started\n\nFollow these steps...".getBytes());
        files.put("test/MainTest.java", "public class MainTest { @Test void testMain() { assertTrue(true); } }".getBytes());

        // Add some binary data
        var binary1 = new byte[1000];
        random.nextBytes(binary1);
        files.put("assets/image.bin", binary1);

        var binary2 = new byte[5000];
        random.nextBytes(binary2);
        files.put("assets/data.bin", binary2);

        return files;
    }

    private List<String> listAllFiles(Path dir) throws IOException {
        var files = new ArrayList<String>();
        listFilesRecursive(dir, dir, files);
        return files;
    }

    private void listFilesRecursive(Path baseDir, Path currentDir, List<String> files) throws IOException {
        try (var stream = Files.newDirectoryStream(currentDir)) {
            for (var entry : stream) {
                if (Files.isDirectory(entry)) {
                    listFilesRecursive(baseDir, entry, files);
                } else {
                    // Get relative path from base
                    var relativePath = baseDir.relativize(entry).toString();
                    files.add(relativePath);
                }
            }
        }
    }

    private List<String> selectFilesToDelete(List<String> files, int percentToDelete) {
        var count = (files.size() * percentToDelete) / 100;
        var shuffled = new ArrayList<>(files);
        Collections.shuffle(shuffled, new Random(123)); // Fixed seed for reproducibility
        return shuffled.subList(0, count);
    }
}
