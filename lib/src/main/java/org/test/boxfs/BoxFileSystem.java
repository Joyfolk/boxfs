package org.test.boxfs;

import org.test.boxfs.internal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.PatternSyntaxException;

/**
 * BoxFS file system implementation.
 * Holds internal state and coordinates all file system operations.
 */
public class BoxFileSystem extends FileSystem {

    private final BoxFileSystemProvider provider;
    private final Path containerPath;
    private final ContainerIO containerIO;
    private final InodeTable inodeTable = new InodeTable();
    private final DirectoryTable directoryTable = new DirectoryTable();
    private final SpaceManager spaceManager;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final boolean readOnly;
    private volatile boolean open = true;

    BoxFileSystem(BoxFileSystemProvider provider, Path containerPath, ContainerIO containerIO,
                  boolean readOnly) {
        this.provider = provider;
        this.containerPath = containerPath;
        this.containerIO = containerIO;
        this.readOnly = readOnly;
        this.spaceManager = new SpaceManager(containerIO.getTotalBlocks());
    }

    /**
     * Initializes a new file system (called after creation).
     */
    void initializeNew() throws IOException {
        lock.writeLock().lock();
        try {
            spaceManager.initializeNew(1);
            inodeTable.createRootInode();
            persistMetadata();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Loads metadata from an existing container.
     */
    void loadMetadata() throws IOException {
        lock.writeLock().lock();
        try {
            var metadataExtents = containerIO.getSuperblock().getMetadataExtents();
            if (metadataExtents.isEmpty()) {
                throw new IOException("No metadata extents in container");
            }

            var totalSize = (int) metadataExtents.stream()
                    .mapToLong(e -> e.sizeInBytes(containerIO.getBlockSize()))
                    .sum();

            var metadataBytes = new byte[totalSize];
            var offset = 0;

            for (var extent : metadataExtents) {
                var extentData = containerIO.readBlocks(extent.startBlock(), extent.blockCount());
                var bytesToCopy = Math.min(extentData.length, totalSize - offset);
                System.arraycopy(extentData, 0, metadataBytes, offset, bytesToCopy);
                offset += bytesToCopy;
            }

            MetadataSerializer.deserialize(metadataBytes, inodeTable, directoryTable, spaceManager);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Persists metadata to the container.
     */
    void persistMetadata() throws IOException {
        if (readOnly) {
            return;
        }

        var metadataBytes = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager);
        var blockSize = containerIO.getBlockSize();
        var blocksNeeded = (metadataBytes.length + blockSize - 1) / blockSize;

        var currentExtents = containerIO.getSuperblock().getMetadataExtents();
        var currentBlocks = currentExtents.stream().mapToInt(Extent::blockCount).sum();

        if (blocksNeeded > currentBlocks) {
            for (var extent : currentExtents) {
                spaceManager.free(extent);
            }

            var newExtents = spaceManager.allocateMultiple(blocksNeeded);
            if (newExtents.isEmpty()) {
                throw new IOException("Not enough space for metadata");
            }

            containerIO.getSuperblock().setMetadataExtents(newExtents);
            currentExtents = newExtents;
            metadataBytes = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager);
        }

        var offset = 0;
        for (var extent : currentExtents) {
            var extentSize = extent.blockCount() * blockSize;
            var bytesToWrite = Math.min(extentSize, metadataBytes.length - offset);

            if (bytesToWrite > 0) {
                var extentData = new byte[extentSize];
                System.arraycopy(metadataBytes, offset, extentData, 0, bytesToWrite);
                containerIO.writeBlocks(extent.startBlock(), extentData);
                offset += bytesToWrite;
            }
        }

        containerIO.writeSuperblock();
    }

    Optional<Long> resolvePathToInodeId(BoxPath path) {
        lock.readLock().lock();
        try {
            var absPath = (BoxPath) path.toAbsolutePath();
            var components = absPath.getComponents();

            if (components.isEmpty()) {
                return Optional.of(InodeTable.ROOT_INODE_ID);
            }

            var currentId = InodeTable.ROOT_INODE_ID;
            for (var component : components) {
                var childId = directoryTable.lookup(currentId, component);
                if (childId.isEmpty()) {
                    return Optional.empty();
                }
                currentId = childId.get();
            }

            return Optional.of(currentId);
        } finally {
            lock.readLock().unlock();
        }
    }

    Optional<Inode> resolvePathToInode(BoxPath path) {
        return resolvePathToInodeId(path).flatMap(inodeTable::get);
    }

    Inode createFile(BoxPath path) throws IOException {
        lock.writeLock().lock();
        try {
            checkWritable();

            var absPath = (BoxPath) path.toAbsolutePath();
            var parent = (BoxPath) absPath.getParent();
            var fileName = absPath.getFileName().toString();

            if (parent == null) {
                throw new IOException("Cannot create file at root");
            }

            var parentInode = resolvePathToInode(parent);
            if (parentInode.isEmpty()) {
                throw new NoSuchFileException(parent.toString());
            }
            if (!parentInode.get().isDirectory()) {
                throw new NotDirectoryException(parent.toString());
            }

            if (directoryTable.lookup(parentInode.get().getId(), fileName).isPresent()) {
                throw new FileAlreadyExistsException(path.toString());
            }

            var fileInode = inodeTable.createInode(Inode.Type.FILE);
            directoryTable.addEntry(new DirectoryEntry(
                    parentInode.get().getId(), fileName, fileInode.getId()));

            return fileInode;
        } finally {
            lock.writeLock().unlock();
        }
    }

    void createDirectory(BoxPath path) throws IOException {
        lock.writeLock().lock();
        try {
            checkWritable();

            var absPath = (BoxPath) path.toAbsolutePath();

            if (absPath.isRoot()) {
                throw new FileAlreadyExistsException("/");
            }

            var parent = (BoxPath) absPath.getParent();
            var dirName = absPath.getFileName().toString();

            var parentInode = resolvePathToInode(parent);
            if (parentInode.isEmpty()) {
                throw new NoSuchFileException(parent.toString());
            }
            if (!parentInode.get().isDirectory()) {
                throw new NotDirectoryException(parent.toString());
            }

            if (directoryTable.lookup(parentInode.get().getId(), dirName).isPresent()) {
                throw new FileAlreadyExistsException(path.toString());
            }

            var dirInode = inodeTable.createInode(Inode.Type.DIRECTORY);
            directoryTable.addEntry(new DirectoryEntry(
                    parentInode.get().getId(), dirName, dirInode.getId()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    void delete(BoxPath path) throws IOException {
        lock.writeLock().lock();
        try {
            checkWritable();

            var absPath = (BoxPath) path.toAbsolutePath();

            if (absPath.isRoot()) {
                throw new IOException("Cannot delete root directory");
            }

            var inodeIdOpt = resolvePathToInodeId(absPath);
            if (inodeIdOpt.isEmpty()) {
                throw new NoSuchFileException(path.toString());
            }

            var inodeId = inodeIdOpt.get();
            var inode = inodeTable.get(inodeId).orElseThrow();

            if (inode.isDirectory() && directoryTable.hasChildren(inodeId)) {
                throw new DirectoryNotEmptyException(path.toString());
            }

            var parent = (BoxPath) absPath.getParent();
            var name = absPath.getFileName().toString();
            var parentId = resolvePathToInodeId(parent).orElseThrow();

            if (!inode.getExtents().isEmpty()) {
                spaceManager.freeAll(inode.getExtents());
            }

            directoryTable.removeEntry(parentId, name);
            inodeTable.remove(inodeId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void move(BoxPath source, BoxPath target, CopyOption... options) throws IOException {
        lock.writeLock().lock();
        try {
            checkWritable();

            var absSource = (BoxPath) source.toAbsolutePath();
            var absTarget = (BoxPath) target.toAbsolutePath();

            if (absSource.isRoot()) {
                throw new IOException("Cannot move root directory");
            }

            var sourceIdOpt = resolvePathToInodeId(absSource);
            if (sourceIdOpt.isEmpty()) {
                throw new NoSuchFileException(source.toString());
            }

            var targetParent = (BoxPath) absTarget.getParent();
            if (targetParent == null) {
                throw new IOException("Invalid target path");
            }

            var targetParentIdOpt = resolvePathToInodeId(targetParent);
            if (targetParentIdOpt.isEmpty()) {
                throw new NoSuchFileException(targetParent.toString());
            }

            var targetParentInode = inodeTable.get(targetParentIdOpt.get()).orElseThrow();
            if (!targetParentInode.isDirectory()) {
                throw new NotDirectoryException(targetParent.toString());
            }

            var targetName = absTarget.getFileName().toString();
            var targetIdOpt = directoryTable.lookup(targetParentIdOpt.get(), targetName);
            var replaceExisting = Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING);

            if (targetIdOpt.isPresent()) {
                if (!replaceExisting) {
                    throw new FileAlreadyExistsException(target.toString());
                }

                var targetInode = inodeTable.get(targetIdOpt.get()).orElseThrow();
                if (targetInode.isDirectory() && directoryTable.hasChildren(targetIdOpt.get())) {
                    throw new DirectoryNotEmptyException(target.toString());
                }

                if (!targetInode.getExtents().isEmpty()) {
                    spaceManager.freeAll(targetInode.getExtents());
                }

                directoryTable.removeEntry(targetParentIdOpt.get(), targetName);
                inodeTable.remove(targetIdOpt.get());
            }

            var sourceParent = (BoxPath) absSource.getParent();
            var sourceName = absSource.getFileName().toString();
            var sourceParentId = resolvePathToInodeId(sourceParent).orElseThrow();

            directoryTable.move(sourceParentId, sourceName, targetParentIdOpt.get(), targetName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void checkAccess(BoxPath path, AccessMode... modes) throws IOException {
        lock.readLock().lock();
        try {
            var inodeOpt = resolvePathToInode((BoxPath) path.toAbsolutePath());
            if (inodeOpt.isEmpty()) {
                throw new NoSuchFileException(path.toString());
            }

            for (var mode : modes) {
                if (mode == AccessMode.WRITE && readOnly) {
                    throw new AccessDeniedException(path.toString(), null, "Read-only file system");
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    BasicFileAttributes readAttributes(BoxPath path) throws IOException {
        lock.readLock().lock();
        try {
            var inodeOpt = resolvePathToInode((BoxPath) path.toAbsolutePath());
            if (inodeOpt.isEmpty()) {
                throw new NoSuchFileException(path.toString());
            }
            return new BoxBasicFileAttributes(inodeOpt.get());
        } finally {
            lock.readLock().unlock();
        }
    }

    List<Path> listDirectory(BoxPath path) throws IOException {
        lock.readLock().lock();
        try {
            var absPath = (BoxPath) path.toAbsolutePath();
            var inodeOpt = resolvePathToInode(absPath);

            if (inodeOpt.isEmpty()) {
                throw new NoSuchFileException(path.toString());
            }
            if (!inodeOpt.get().isDirectory()) {
                throw new NotDirectoryException(path.toString());
            }

            var entries = directoryTable.listChildren(inodeOpt.get().getId());
            var children = new ArrayList<Path>();

            for (var entry : entries) {
                children.add(absPath.resolve(entry.name()));
            }

            return children;
        } finally {
            lock.readLock().unlock();
        }
    }

    int readFileData(Inode inode, long position, ByteBuffer dest) throws IOException {
        lock.readLock().lock();
        try {
            if (position >= inode.getSize()) {
                return -1;
            }

            var extents = inode.getExtents();
            var blockSize = containerIO.getBlockSize();
            var totalBytesRead = 0;
            var currentPosition = position;
            var remainingInFile = inode.getSize() - position;

            for (var extent : extents) {
                var extentStartByte = 0L;
                for (var e : extents) {
                    if (e == extent) break;
                    extentStartByte += e.sizeInBytes(blockSize);
                }

                var extentEndByte = extentStartByte + extent.sizeInBytes(blockSize);

                if (currentPosition >= extentEndByte) {
                    continue;
                }

                if (currentPosition < extentStartByte) {
                    continue;
                }

                var offsetInExtent = currentPosition - extentStartByte;
                var bytesAvailableInExtent = extent.sizeInBytes(blockSize) - offsetInExtent;
                var bytesToRead = (int) Math.min(
                        Math.min(bytesAvailableInExtent, dest.remaining()),
                        remainingInFile - totalBytesRead
                );

                if (bytesToRead <= 0) {
                    break;
                }

                var readBuffer = ByteBuffer.allocate(bytesToRead);
                var bytesRead = containerIO.readFromExtent(extent, offsetInExtent, readBuffer);

                if (bytesRead > 0) {
                    readBuffer.flip();
                    var actualBytes = (int) Math.min(bytesRead, remainingInFile - totalBytesRead);
                    readBuffer.limit(actualBytes);
                    dest.put(readBuffer);
                    totalBytesRead += actualBytes;
                    currentPosition += actualBytes;
                }

                if (!dest.hasRemaining() || totalBytesRead >= remainingInFile) {
                    break;
                }
            }

            return totalBytesRead > 0 ? totalBytesRead : -1;
        } finally {
            lock.readLock().unlock();
        }
    }

    int writeFileData(Inode inode, long position, ByteBuffer src) throws IOException {
        lock.writeLock().lock();
        try {
            checkWritable();

            var blockSize = containerIO.getBlockSize();
            var bytesToWrite = src.remaining();
            var endPosition = position + bytesToWrite;

            var currentAllocatedBytes = inode.getExtents().stream()
                    .mapToLong(e -> e.sizeInBytes(blockSize))
                    .sum();

            if (endPosition > currentAllocatedBytes) {
                var additionalBytesNeeded = (int) (endPosition - currentAllocatedBytes);
                var additionalBlocksNeeded = (additionalBytesNeeded + blockSize - 1) / blockSize;

                var newExtents = spaceManager.allocateMultiple(additionalBlocksNeeded);
                if (newExtents.isEmpty()) {
                    throw new IOException("No space available");
                }

                for (var extent : newExtents) {
                    inode.addExtent(extent);
                }
            }

            var extents = inode.getExtents();
            var totalBytesWritten = 0;
            var currentPosition = position;

            for (var extent : extents) {
                var extentStartByte = 0L;
                for (var e : extents) {
                    if (e == extent) break;
                    extentStartByte += e.sizeInBytes(blockSize);
                }

                var extentEndByte = extentStartByte + extent.sizeInBytes(blockSize);

                if (currentPosition >= extentEndByte) {
                    continue;
                }

                if (currentPosition < extentStartByte) {
                    continue;
                }

                var offsetInExtent = currentPosition - extentStartByte;
                var bytesAvailableInExtent = extent.sizeInBytes(blockSize) - offsetInExtent;
                var bytesForThisExtent = (int) Math.min(bytesAvailableInExtent, src.remaining());

                if (bytesForThisExtent <= 0) {
                    break;
                }

                var written = containerIO.writeToExtent(extent, offsetInExtent, src);
                totalBytesWritten += written;
                currentPosition += written;

                if (!src.hasRemaining()) {
                    break;
                }
            }

            if (endPosition > inode.getSize()) {
                inode.setSize(endPosition);
            }

            return totalBytesWritten;
        } finally {
            lock.writeLock().unlock();
        }
    }

    void truncateFile(Inode inode, long newSize) throws IOException {
        lock.writeLock().lock();
        try {
            checkWritable();

            if (newSize >= inode.getSize()) {
                return;
            }

            var blockSize = containerIO.getBlockSize();
            var blocksNeeded = (newSize + blockSize - 1) / blockSize;

            var currentExtents = new ArrayList<>(inode.getExtents());
            var keepExtents = new ArrayList<Extent>();
            var freeExtents = new ArrayList<Extent>();

            var accumulatedBlocks = 0L;
            for (var extent : currentExtents) {
                if (accumulatedBlocks >= blocksNeeded) {
                    freeExtents.add(extent);
                } else if (accumulatedBlocks + extent.blockCount() <= blocksNeeded) {
                    keepExtents.add(extent);
                    accumulatedBlocks += extent.blockCount();
                } else {
                    var blocksToKeep = (int) (blocksNeeded - accumulatedBlocks);
                    if (blocksToKeep > 0) {
                        keepExtents.add(new Extent(extent.startBlock(), blocksToKeep));
                    }
                    var blocksToFree = extent.blockCount() - blocksToKeep;
                    if (blocksToFree > 0) {
                        freeExtents.add(new Extent(extent.startBlock() + blocksToKeep, blocksToFree));
                    }
                    accumulatedBlocks += blocksToKeep;
                }
            }

            inode.setExtents(keepExtents);
            inode.setSize(newSize);

            if (!freeExtents.isEmpty()) {
                spaceManager.freeAll(freeExtents);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        if (open) {
            lock.writeLock().lock();
            try {
                if (open) {
                    open = false;
                    persistMetadata();
                    containerIO.sync();
                    containerIO.close();
                    provider.removeFileSystem(containerPath);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return List.of(new BoxPath(this, "/"));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return List.of(new BoxFileStore(this));
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Set.of("basic");
    }

    @Override
    public Path getPath(String first, String... more) {
        var sb = new StringBuilder(first);
        for (var part : more) {
            if (!sb.isEmpty() && !sb.toString().endsWith("/")) {
                sb.append("/");
            }
            sb.append(part);
        }
        return new BoxPath(this, sb.toString());
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        var parts = syntaxAndPattern.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid pattern: " + syntaxAndPattern);
        }

        var syntax = parts[0];
        var pattern = parts[1];

        if (!"glob".equals(syntax) && !"regex".equals(syntax)) {
            throw new UnsupportedOperationException("Unsupported syntax: " + syntax);
        }

        java.util.regex.Pattern regex;
        if ("glob".equals(syntax)) {
            regex = globToRegex(pattern);
        } else {
            try {
                regex = java.util.regex.Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex pattern", e);
            }
        }

        return path -> regex.matcher(path.toString()).matches();
    }

    private java.util.regex.Pattern globToRegex(String glob) {
        var regex = new StringBuilder("^");
        for (var i = 0; i < glob.length(); i++) {
            var c = glob.charAt(i);
            switch (c) {
                case '*' -> {
                    if (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        regex.append(".*");
                        i++;
                    } else {
                        regex.append("[^/]*");
                    }
                }
                case '?' -> regex.append("[^/]");
                case '.', '(', ')', '+', '|', '^', '$', '@', '%' -> regex.append("\\").append(c);
                case '\\' -> {
                    if (i + 1 < glob.length()) {
                        regex.append("\\").append(glob.charAt(++i));
                    }
                }
                case '[', ']' -> regex.append(c);
                case '{' -> regex.append("(");
                case '}' -> regex.append(")");
                case ',' -> regex.append("|");
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return java.util.regex.Pattern.compile(regex.toString());
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("User principals not supported");
    }

    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("Watch service not supported");
    }

    Path getContainerPath() {
        return containerPath;
    }

    int getBlockSize() {
        return containerIO.getBlockSize();
    }

    long getTotalBlocks() {
        return containerIO.getTotalBlocks();
    }

    long getFreeBlocks() {
        lock.readLock().lock();
        try {
            return spaceManager.getTotalFreeBlocks();
        } finally {
            lock.readLock().unlock();
        }
    }

    long getUsedBlocks() {
        lock.readLock().lock();
        try {
            return spaceManager.getTotalUsedBlocks();
        } finally {
            lock.readLock().unlock();
        }
    }

    void checkOpen() throws IOException {
        if (!open) {
            throw new ClosedFileSystemException();
        }
    }

    void checkWritable() throws IOException {
        checkOpen();
        if (readOnly) {
            throw new IOException("File system is read-only");
        }
    }

    void sync() throws IOException {
        lock.writeLock().lock();
        try {
            persistMetadata();
            containerIO.sync();
        } finally {
            lock.writeLock().unlock();
        }
    }

    Optional<Inode> getInode(long id) {
        lock.readLock().lock();
        try {
            return inodeTable.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    ReentrantReadWriteLock getLock() {
        return lock;
    }
}
