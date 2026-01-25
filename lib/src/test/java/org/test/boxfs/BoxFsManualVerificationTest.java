package org.test.boxfs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Manual verification test matching the plan's verification code.
 */
class BoxFsManualVerificationTest {

    @TempDir
    Path tempDir;

    @Test
    void manualVerification() throws IOException {
        var containerPath = tempDir.resolve("test.box");
        var uri = URI.create("box:" + containerPath);

        try (var fs = FileSystems.newFileSystem(uri,
                Map.of("create", "true", "totalBlocks", 256L))) {

            var dir = fs.getPath("/test");
            Files.createDirectory(dir);

            var file = dir.resolve("hello.txt");
            Files.write(file, "Hello BoxFS!".getBytes());

            var content = Files.readString(file);
            assertEquals("Hello BoxFS!", content);

            Files.delete(file);
            assertFalse(Files.exists(file));

            Files.delete(dir);
            assertFalse(Files.exists(dir));
        }
    }
}
