package org.test.boxfs;

import org.test.boxfs.internal.ContainerIO;
import org.test.boxfs.internal.Superblock;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FileSystemProvider implementation for BoxFS.
 *
 * URI scheme: box:/path/to/container.box
 *
 * Environment options for newFileSystem:
 * - "create" (String "true"/"false"): create new container if it doesn't exist
 * - "totalBlocks" (Long): number of blocks for new container (default: 256)
 * - "blockSize" (Integer): block size in bytes (default: 4096)
 * - "readOnly" (String "true"/"false"): open in read-only mode
 */
public class BoxFileSystemProvider extends FileSystemProvider {

    public static final String SCHEME = "box";

    private static final long DEFAULT_TOTAL_BLOCKS = 256;
    private static final int DEFAULT_BLOCK_SIZE = Superblock.DEFAULT_BLOCK_SIZE;

    private final Map<Path, BoxFileSystem> fileSystems = new ConcurrentHashMap<>();

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        checkUri(uri);

        var containerPath = uriToContainerPath(uri);

        synchronized (fileSystems) {
            if (fileSystems.containsKey(containerPath)) {
                throw new FileSystemAlreadyExistsException(containerPath.toString());
            }

            var create = "true".equals(env.get("create"));
            var readOnly = "true".equals(env.get("readOnly"));

            BoxFileSystem fs;

            if (Files.exists(containerPath)) {
                if (create && env.containsKey("totalBlocks")) {
                    throw new FileAlreadyExistsException(containerPath.toString());
                }
                var containerIO = ContainerIO.open(containerPath, readOnly);
                fs = new BoxFileSystem(this, containerPath, containerIO, readOnly);
                fs.loadMetadata();
            } else {
                if (!create) {
                    throw new NoSuchFileException(containerPath.toString());
                }
                var totalBlocks = getLongEnv(env, "totalBlocks", DEFAULT_TOTAL_BLOCKS);
                var blockSize = getIntEnv(env, "blockSize", DEFAULT_BLOCK_SIZE);

                var containerIO = ContainerIO.create(containerPath, blockSize, totalBlocks);
                fs = new BoxFileSystem(this, containerPath, containerIO, readOnly);
                fs.initializeNew();
            }

            fileSystems.put(containerPath, fs);
            return fs;
        }
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        checkUri(uri);
        var containerPath = uriToContainerPath(uri);

        var fs = fileSystems.get(containerPath);
        if (fs == null) {
            throw new FileSystemNotFoundException(uri.toString());
        }
        return fs;
    }

    @Override
    public Path getPath(URI uri) {
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

        var fs = fileSystems.get(containerPath);
        if (fs == null) {
            throw new FileSystemNotFoundException("No file system for: " + containerPath);
        }

        return fs.getPath(internalPath);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options,
                                               FileAttribute<?>... attrs) throws IOException {
        var boxPath = toBoxPath(path);
        var fs = (BoxFileSystem) boxPath.getFileSystem();
        fs.checkOpen();

        return new BoxSeekableByteChannel(fs, boxPath, options);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException {
        var boxPath = toBoxPath(dir);
        var fs = (BoxFileSystem) boxPath.getFileSystem();
        fs.checkOpen();

        return new BoxDirectoryStream(fs, boxPath, filter);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        var boxPath = toBoxPath(dir);
        var fs = (BoxFileSystem) boxPath.getFileSystem();
        fs.checkOpen();
        fs.createDirectory(boxPath);
    }

    @Override
    public void delete(Path path) throws IOException {
        var boxPath = toBoxPath(path);
        var fs = (BoxFileSystem) boxPath.getFileSystem();
        fs.checkOpen();
        fs.delete(boxPath);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException("Copy not supported - use move or read/write");
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        var boxSource = toBoxPath(source);
        var boxTarget = toBoxPath(target);

        var sourceFs = (BoxFileSystem) boxSource.getFileSystem();
        var targetFs = (BoxFileSystem) boxTarget.getFileSystem();

        if (sourceFs != targetFs) {
            throw new IOException("Cannot move between different file systems");
        }

        sourceFs.checkOpen();
        sourceFs.move(boxSource, boxTarget, options);
    }

    @Override
    public boolean isSameFile(Path path1, Path path2) throws IOException {
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
    public boolean isHidden(Path path) {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        var boxPath = toBoxPath(path);
        var fs = (BoxFileSystem) boxPath.getFileSystem();
        fs.checkOpen();
        return new BoxFileStore(fs);
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        var boxPath = toBoxPath(path);
        var fs = (BoxFileSystem) boxPath.getFileSystem();
        fs.checkOpen();
        fs.checkAccess(boxPath, modes);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type,
                                                                  LinkOption... options) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type,
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
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
            throws IOException {
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
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
            throws IOException {
        throw new UnsupportedOperationException("Setting attributes not supported");
    }

    void removeFileSystem(Path containerPath) {
        fileSystems.remove(containerPath);
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
