package org.test.boxfs;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.test.boxfs.internal.ContainerIO;
import org.test.boxfs.internal.Inode;
import org.test.boxfs.internal.Superblock;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * FileSystemProvider implementation for BoxFS.
 * <p>
 * URI scheme: box:/path/to/container.box
 * <p>
 * Environment options for the newFileSystem:
 * - "create" (String "true"): create a new container if it doesn't exist
 * - "totalBlocks" (Long): number of blocks for the new container (default: 256)
 * - "blockSize" (Integer): block size in bytes (default: 4096)
 */
public class BoxFileSystemProvider extends FileSystemProvider {
  private static final String SCHEME = "box";
  private static final long DEFAULT_TOTAL_BLOCKS = 256;
  private static final int DEFAULT_BLOCK_SIZE = Superblock.DEFAULT_BLOCK_SIZE;

  private final Map<Path, BoxFileSystem> fileSystems = new HashMap<>();

  @Override
  public @NotNull String getScheme() {
    return SCHEME;
  }

  @Override
  public @NotNull FileSystem newFileSystem(@NotNull URI uri, @NotNull Map<String, ?> env) throws IOException {
    checkUri(uri);

    var containerPath = uriToContainerPath(uri);

    synchronized (fileSystems) {
      if (fileSystems.containsKey(containerPath)) {
        throw new FileSystemAlreadyExistsException(containerPath.toString());
      }

      var create = "true".equals(env.get("create"));

      BoxFileSystem fs;

      if (Files.exists(containerPath)) {
        var containerIO = ContainerIO.open(containerPath);
        fs = new BoxFileSystem(this, containerPath, containerIO);
        fs.loadMetadata();
      } else {
        if (!create) {
          throw new NoSuchFileException(containerPath.toString());
        }
        var totalBlocks = getLongEnv(env, "totalBlocks", DEFAULT_TOTAL_BLOCKS);
        var blockSize = getIntEnv(env, "blockSize", DEFAULT_BLOCK_SIZE);

        var containerIO = ContainerIO.create(containerPath, blockSize, totalBlocks);
        fs = new BoxFileSystem(this, containerPath, containerIO);
        fs.initializeNew();
      }

      fileSystems.put(containerPath, fs);
      return fs;
    }
  }

  @Override
  public @NotNull FileSystem getFileSystem(@NotNull URI uri) {
    checkUri(uri);
    return getFileSystemByPath(uriToContainerPath(uri));
  }

  private BoxFileSystem getFileSystemByPath(Path containerPath) {
    synchronized (fileSystems) {
      var fs = fileSystems.get(containerPath);
      if (fs == null) {
        throw new FileSystemNotFoundException(containerPath.toString());
      }
      return fs;
    }
  }

  @Override
  public @NotNull Path getPath(@NotNull URI uri) {
    checkUri(uri);

    var schemeSpecific = uri.getSchemeSpecificPart();
    var bangIndex = schemeSpecific.indexOf('!');

    Path containerPath;
    String internalPath;

    if (bangIndex >= 0) {
      containerPath = Path.of(schemeSpecific.substring(0, bangIndex));
      internalPath = schemeSpecific.substring(bangIndex + 1);
    } else {
      containerPath = Path.of(schemeSpecific);
      internalPath = "/";
    }

    var fs = getFileSystemByPath(containerPath);
    return fs.getPath(internalPath);
  }

  @Override
  public @NotNull SeekableByteChannel newByteChannel(@NotNull Path path, @NotNull Set<? extends OpenOption> options,
                                                     FileAttribute<?>... attrs) throws IOException {
    var boxPath = toBoxPath(path);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();

    return new BoxSeekableByteChannel(fs, boxPath, options);
  }

  @Override
  public @NotNull DirectoryStream<Path> newDirectoryStream(@NotNull Path dir, @NotNull DirectoryStream.Filter<? super Path> filter)
    throws IOException {
    var boxPath = toBoxPath(dir);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();

    return new BoxDirectoryStream(fs, boxPath, filter);
  }

  @Override
  public void createDirectory(@NotNull Path dir, FileAttribute<?>... attrs) throws IOException {
    var boxPath = toBoxPath(dir);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();
    fs.createEntry(boxPath, Inode.Type.DIRECTORY);
  }

  @Override
  public void delete(@NotNull Path path) throws IOException {
    var boxPath = toBoxPath(path);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();
    fs.delete(boxPath);
  }

  /**
   * Copies a file.
   * <p>
   * Supports both same-container and cross-container copy operations.
   * For cross-container copies, data is streamed from source to target.
   * <p>
   * The JDK handles cross-provider copy (e.g., BoxFS to OS filesystem) automatically
   * via the java.nio.file.CopyMoveHelper.copyToForeignTarget() method.
   */
  @Override
  public void copy(@NotNull Path source, @NotNull Path target, CopyOption... options) throws IOException {
    var boxSource = toBoxPath(source);
    var boxTarget = toBoxPath(target);

    var sourceFs = (BoxFileSystem) boxSource.getFileSystem();
    var targetFs = (BoxFileSystem) boxTarget.getFileSystem();

    sourceFs.checkOpen();
    targetFs.checkOpen();

    if (sourceFs == targetFs) {
      sourceFs.copy(boxSource, boxTarget, options);
    } else {
      crossContainerCopy(boxSource, boxTarget, options);
    }
  }

  private void crossContainerCopy(BoxPath source, BoxPath target, CopyOption... options) throws IOException {
    var replaceExisting = replaceExisting(options);

    if (Files.exists(target) && !replaceExisting) {
      throw new FileAlreadyExistsException(target.toString());
    }

    var openOptions = replaceExisting
            ? new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE}
            : new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE};

    try (var in = Files.newInputStream(source);
         var out = Files.newOutputStream(target, openOptions)) {
      in.transferTo(out);
    }
  }

  private boolean replaceExisting(CopyOption[] options) {
    for (var option : options) {
      if (option == StandardCopyOption.REPLACE_EXISTING) {
        return true;
      }
    }
    return false;
  }

  /**
   * Moves/renames a file.
   * <p>
   * Supports both same-container and cross-container move operations.
   * For same-container moves, this is an efficient metadata-only operation.
   * For cross-container moves, data is copied then deleted from the source.
   * <p>
   * The JDK handles cross-provider copy (e.g., BoxFS to OS filesystem) automatically
   * via the java.nio.file.CopyMoveHelper.copyToForeignTarget() method.
   */
  @Override
  public void move(@NotNull Path source, @NotNull Path target, CopyOption... options) throws IOException {
    var boxSource = toBoxPath(source);
    var boxTarget = toBoxPath(target);

    var sourceFs = (BoxFileSystem) boxSource.getFileSystem();
    var targetFs = (BoxFileSystem) boxTarget.getFileSystem();

    sourceFs.checkOpen();
    targetFs.checkOpen();

    if (sourceFs == targetFs) {
      sourceFs.move(boxSource, boxTarget, options);
    } else {
      crossContainerCopy(boxSource, boxTarget, options);
      sourceFs.delete(boxSource);
    }
  }

  @Override
  public boolean isSameFile(@NotNull Path path1, @NotNull Path path2) {
    if (path1.equals(path2)) {
      return true;
    }

    if (!(path1 instanceof BoxPath box1) || !(path2 instanceof BoxPath box2)) {
      return false;
    }

    if (box1.getFileSystem() != box2.getFileSystem()) {
      return false;
    }

    return box1.toAbsolutePath().equals(box2.toAbsolutePath());
  }

  @Override
  public boolean isHidden(@NotNull Path path) {
    return false;
  }

  @Override
  public @NotNull FileStore getFileStore(@NotNull Path path) {
    var boxPath = toBoxPath(path);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();
    return new BoxFileStore(fs);
  }

  @Override
  public void checkAccess(@NotNull Path path, AccessMode... modes) throws IOException {
    var boxPath = toBoxPath(path);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();
    fs.checkAccess(boxPath, modes);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V extends FileAttributeView> @Nullable V getFileAttributeView(@NotNull Path path, @NotNull Class<V> type,
                                                                        LinkOption... options) {
    if (type == BasicFileAttributeView.class) {
      var boxPath = toBoxPath(path);
      var fs = (BoxFileSystem) boxPath.getFileSystem();
      return (V) new BoxBasicFileAttributeView(fs, boxPath);
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <A extends BasicFileAttributes> @NotNull A readAttributes(@NotNull Path path, @NotNull Class<A> type,
                                                                   LinkOption... options) throws IOException {
    if (type != BasicFileAttributes.class && type != BoxBasicFileAttributes.class) {
      throw new UnsupportedOperationException("Only BasicFileAttributes supported");
    }

    var boxPath = toBoxPath(path);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();

    return (A) fs.readAttributes(boxPath);
  }

  @Override
  public @NotNull Map<String, Object> readAttributes(@NotNull Path path, @NotNull String attributes,
                                                     LinkOption... options) throws IOException {
    var boxPath = toBoxPath(path);
    var fs = (BoxFileSystem) boxPath.getFileSystem();
    fs.checkOpen();

    var attrs = fs.readAttributes(boxPath);

    String view;
    String attrList;
    var colonPos = attributes.indexOf(':');
    if (colonPos >= 0) {
      view = attributes.substring(0, colonPos);
      attrList = attributes.substring(colonPos + 1);
    } else {
      view = "basic";
      attrList = attributes;
    }

    if (!"basic".equals(view)) {
      throw new UnsupportedOperationException("View not supported: " + view);
    }

    var result = new java.util.HashMap<String, Object>();

    if ("*".equals(attrList)) {
      result.put("lastModifiedTime", attrs.lastModifiedTime());
      result.put("lastAccessTime", attrs.lastAccessTime());
      result.put("creationTime", attrs.creationTime());
      result.put("isRegularFile", attrs.isRegularFile());
      result.put("isDirectory", attrs.isDirectory());
      result.put("isSymbolicLink", attrs.isSymbolicLink());
      result.put("isOther", attrs.isOther());
      result.put("size", attrs.size());
      result.put("fileKey", attrs.fileKey());
    } else {
      for (String attr : attrList.split(",")) {
        switch (attr.trim()) {
          case "lastModifiedTime" -> result.put("lastModifiedTime", attrs.lastModifiedTime());
          case "lastAccessTime" -> result.put("lastAccessTime", attrs.lastAccessTime());
          case "creationTime" -> result.put("creationTime", attrs.creationTime());
          case "isRegularFile" -> result.put("isRegularFile", attrs.isRegularFile());
          case "isDirectory" -> result.put("isDirectory", attrs.isDirectory());
          case "isSymbolicLink" -> result.put("isSymbolicLink", attrs.isSymbolicLink());
          case "isOther" -> result.put("isOther", attrs.isOther());
          case "size" -> result.put("size", attrs.size());
          case "fileKey" -> result.put("fileKey", attrs.fileKey());
          default -> throw new IllegalArgumentException("Unknown attribute: " + attr);
        }
      }
    }

    return result;
  }

  @Override
  public void setAttribute(@NotNull Path path, @NotNull String attribute, @NotNull Object value,
                           LinkOption... options) {
    throw new UnsupportedOperationException("Setting attributes not supported");
  }

  void removeFileSystem(Path containerPath) {
    synchronized (fileSystems) {
      //noinspection resource
      fileSystems.remove(containerPath);
    }
  }

  private void checkUri(URI uri) {
    if (!SCHEME.equals(uri.getScheme())) {
      throw new IllegalArgumentException("URI scheme must be '" + SCHEME + "'");
    }
  }

  private Path uriToContainerPath(URI uri) {
    var schemeSpecific = uri.getSchemeSpecificPart();
    var bangIndex = schemeSpecific.indexOf('!');
    if (bangIndex >= 0) {
      return Path.of(schemeSpecific.substring(0, bangIndex));
    }
    return Path.of(schemeSpecific);
  }

  private BoxPath toBoxPath(Path path) {
    if (path instanceof BoxPath boxPath) {
      return boxPath;
    }
    throw new IllegalArgumentException("Path is not a BoxPath: " + path);
  }

  @SuppressWarnings("SameParameterValue")
  private long getLongEnv(Map<String, ?> env, String key, long defaultValue) {
    var value = env.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    return Long.parseLong(value.toString());
  }

  @SuppressWarnings("SameParameterValue")
  private int getIntEnv(Map<String, ?> env, String key, int defaultValue) {
    var value = env.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    return Integer.parseInt(value.toString());
  }
}
