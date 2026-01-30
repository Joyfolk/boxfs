package org.test.boxfs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple facade API for the BoxFS single-container file system.
 *
 * <p>This class provides a straightforward API for file system operations,
 * hiding the complexity of the underlying NIO FileSystemProvider implementation.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Create a new container
 * try (BoxFs fs = BoxFs.create(Path.of("mydata.box"))) {
 *     fs.createDirectory("/documents");
 *
 *     // Write a file using streaming
 *     try (OutputStream out = fs.openWrite("/documents/hello.txt")) {
 *         out.write("Hello, World!".getBytes());
 *     }
 *
 *     // Read a file using streaming
 *     try (InputStream in = fs.openRead("/documents/hello.txt")) {
 *         byte[] content = in.readAllBytes();
 *     }
 * }
 *
 * // Open an existing container
 * try (BoxFs fs = BoxFs.open(Path.of("mydata.box"))) {
 *     List<String> files = fs.listDirectory("/documents");
 * }
 * }</pre>
 */
public class BoxFs implements Closeable {

  private static final long DEFAULT_TOTAL_BLOCKS = 256L;
  private static final int DEFAULT_BLOCK_SIZE = 4096;

  private final FileSystem fileSystem;

  private BoxFs(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  /**
   * Creates a new container file with default settings (1MB capacity).
   *
   * @param containerPath path to the container file on the host filesystem
   * @return a new BoxFs instance
   * @throws IOException if the container cannot be created
   */
  public static BoxFs create(Path containerPath) throws IOException {
    return create(containerPath, DEFAULT_TOTAL_BLOCKS, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Creates a new container file with specified capacity.
   *
   * @param containerPath path to the container file on the host filesystem
   * @param totalBlocks   number of blocks in the container
   * @return a new BoxFs instance
   * @throws IOException if the container cannot be created
   */
  public static BoxFs create(Path containerPath, long totalBlocks) throws IOException {
    return create(containerPath, totalBlocks, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Creates a new container file with specified capacity and block size.
   *
   * @param containerPath path to the container file on the host filesystem
   * @param totalBlocks   number of blocks in the container
   * @param blockSize     size of each block in bytes
   * @return a new BoxFs instance
   * @throws IOException if the container cannot be created
   */
  public static BoxFs create(Path containerPath, long totalBlocks, int blockSize) throws IOException {
    var uri = URI.create("box:" + containerPath.toAbsolutePath());
    var env = Map.of(
      "create", "true",
      "totalBlocks", totalBlocks,
      "blockSize", blockSize
    );
    var fs = FileSystems.newFileSystem(uri, env);
    return new BoxFs(fs);
  }

  /**
   * Opens an existing container file.
   *
   * @param containerPath path to the container file on the host filesystem
   * @return a BoxFs instance for the existing container
   * @throws IOException if the container cannot be opened
   */
  public static BoxFs open(Path containerPath) throws IOException {
    var uri = URI.create("box:" + containerPath.toAbsolutePath());
    var fs = FileSystems.newFileSystem(uri, Map.of());
    return new BoxFs(fs);
  }

  // ==================== File Operations ====================

  /**
   * Creates a new empty file.
   *
   * @param path absolute path within the container (e.g., "/documents/file.txt")
   * @throws IOException if the file cannot be created
   */
  public void createFile(String path) throws IOException {
    Files.createFile(resolvePath(path));
  }

  /**
   * Opens a file for reading.
   *
   * @param path absolute path within the container
   * @return an InputStream to read the file content
   * @throws IOException if the file cannot be opened
   */
  public InputStream openRead(String path) throws IOException {
    return Files.newInputStream(resolvePath(path));
  }

  /**
   * Opens a file for writing, creating it if it doesn't exist or truncating if it does.
   *
   * @param path absolute path within the container
   * @return an OutputStream to write the file content
   * @throws IOException if the file cannot be opened
   */
  public OutputStream openWrite(String path) throws IOException {
    var filePath = resolvePath(path);
    var parent = filePath.getParent();
    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent);
    }
    return Files.newOutputStream(filePath);
  }

  /**
   * Opens a file for appending, creating it if it doesn't exist.
   *
   * @param path absolute path within the container
   * @return an OutputStream to append to the file
   * @throws IOException if the file cannot be opened
   */
  public OutputStream openAppend(String path) throws IOException {
    var filePath = resolvePath(path);
    var parent = filePath.getParent();
    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent);
    }
    return Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  /**
   * Deletes a file.
   *
   * @param path absolute path within the container
   * @throws IOException if the file cannot be deleted
   */
  public void deleteFile(String path) throws IOException {
    Files.delete(resolvePath(path));
  }

  /**
   * Renames a file (within the same directory).
   *
   * @param source current path
   * @param target new path
   * @throws IOException if the rename fails
   */
  public void renameFile(String source, String target) throws IOException {
    Files.move(resolvePath(source), resolvePath(target));
  }

  /**
   * Moves a file to a different location.
   *
   * @param source current path
   * @param target new path
   * @throws IOException if the move fails
   */
  public void moveFile(String source, String target) throws IOException {
    Files.move(resolvePath(source), resolvePath(target));
  }

  // ==================== Directory Operations ====================

  /**
   * Creates a directory.
   *
   * @param path absolute path within the container
   * @throws IOException if the directory cannot be created
   */
  public void createDirectory(String path) throws IOException {
    Files.createDirectory(resolvePath(path));
  }

  /**
   * Creates a directory and any necessary parent directories.
   *
   * @param path absolute path within the container
   * @throws IOException if the directories cannot be created
   */
  public void createDirectories(String path) throws IOException {
    Files.createDirectories(resolvePath(path));
  }

  /**
   * Lists the contents of a directory.
   *
   * @param path absolute path to the directory
   * @return list of entry names (not full paths)
   * @throws IOException if the directory cannot be read
   */
  public List<String> listDirectory(String path) throws IOException {
    var entries = new ArrayList<String>();
    try (var stream = Files.newDirectoryStream(resolvePath(path))) {
      for (var entry : stream) {
        entries.add(entry.getFileName().toString());
      }
    }
    return entries;
  }

  /**
   * Deletes an empty directory.
   *
   * @param path absolute path within the container
   * @throws IOException if the directory cannot be deleted
   */
  public void deleteDirectory(String path) throws IOException {
    Files.delete(resolvePath(path));
  }

  // ==================== Query Operations ====================

  /**
   * Checks if a file or directory exists.
   *
   * @param path absolute path within the container
   * @return true if the path exists
   */
  public boolean exists(String path) {
    return Files.exists(resolvePath(path));
  }

  /**
   * Checks if the path is a regular file.
   *
   * @param path absolute path within the container
   * @return true if the path is a file
   */
  public boolean isFile(String path) {
    return Files.isRegularFile(resolvePath(path));
  }

  /**
   * Checks if the path is a directory.
   *
   * @param path absolute path within the container
   * @return true if the path is a directory
   */
  public boolean isDirectory(String path) {
    return Files.isDirectory(resolvePath(path));
  }

  /**
   * Returns the size of a file in bytes.
   *
   * @param path absolute path within the container
   * @return file size in bytes
   * @throws IOException if the size cannot be determined
   */
  public long size(String path) throws IOException {
    return Files.size(resolvePath(path));
  }

  // ==================== Lifecycle ====================

  /**
   * Flushes any pending changes to disk.
   *
   * @throws IOException if the sync fails
   */
  public void sync() throws IOException {
    ((BoxFileSystem) fileSystem).sync();
  }

  /**
   * Closes the container, persisting all metadata.
   *
   * @throws IOException if the close fails
   */
  @Override
  public void close() throws IOException {
    fileSystem.close();
  }

  /**
   * Returns the underlying NIO FileSystem for advanced operations.
   *
   * @return the NIO FileSystem
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  private Path resolvePath(String path) {
    return fileSystem.getPath(path);
  }
}
