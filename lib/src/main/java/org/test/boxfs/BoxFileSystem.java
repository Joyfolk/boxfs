package org.test.boxfs;

import org.jetbrains.annotations.NotNull;
import org.test.boxfs.internal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
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
  private volatile boolean open = true;

  BoxFileSystem(BoxFileSystemProvider provider, Path containerPath, ContainerIO containerIO) {
    this.provider = provider;
    this.containerPath = containerPath;
    this.containerIO = containerIO;
    this.spaceManager = new SpaceManager(containerIO.getTotalBlocks());
  }

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

  void persistMetadata() throws IOException {
    var blockSize = containerIO.getBlockSize();
    var currentExtents = containerIO.getSuperblock().getMetadataExtents();

    // The Loop here is used to handle a subtle edge case: metadata includes the free extent
    // list, which changes when we allocate blocks for metadata storage. After reallocation,
    // the free list might have MORE extents, making the re-serialized metadata larger than what we allocated space
    // for. So we are reallocating until the serialized metadata fits. In practice,
    // this shouldn't iterate more than once or twice, since free list growth is bounded.
    while (true) {
      var metadataBytes = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager);
      var blocksNeeded = (metadataBytes.length + blockSize - 1) / blockSize;
      var currentBlocks = currentExtents.stream().mapToInt(Extent::blockCount).sum();

      if (blocksNeeded <= currentBlocks) {
        writeMetadataToExtents(metadataBytes, currentExtents, blockSize);
        break;
      }

      for (var extent : currentExtents) {
        spaceManager.free(extent);
      }

      var newExtents = spaceManager.allocateMultiple(blocksNeeded);
      if (newExtents.isEmpty()) {
        throw new IOException("Not enough space for metadata");
      }

      containerIO.getSuperblock().setMetadataExtents(newExtents);
      currentExtents = newExtents;
    }

    containerIO.writeSuperblock();
  }

  private void writeMetadataToExtents(byte[] metadataBytes, List<Extent> extents, int blockSize)
    throws IOException {
    var offset = 0;
    for (var extent : extents) {
      var extentSize = extent.blockCount() * blockSize;
      var bytesToWrite = Math.min(extentSize, metadataBytes.length - offset);

      if (bytesToWrite > 0) {
        var extentData = new byte[extentSize];
        System.arraycopy(metadataBytes, offset, extentData, 0, bytesToWrite);
        containerIO.writeBlocks(extent.startBlock(), extentData);
        offset += bytesToWrite;
      }
    }
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

  Inode createEntry(BoxPath path, Inode.Type type) throws IOException {
    lock.writeLock().lock();
    try {
      checkWritable();

      var absPath = (BoxPath) path.toAbsolutePath();
      if (absPath.isRoot()) {
        throw new FileAlreadyExistsException("/");
      }

      var parent = (BoxPath) absPath.getParent();
      var name = absPath.getFileName().toString();

      var parentInode = resolvePathToInode(parent)
        .orElseThrow(() -> new NoSuchFileException(parent.toString()));
      if (!parentInode.isDirectory()) {
        throw new NotDirectoryException(parent.toString());
      }

      if (directoryTable.lookup(parentInode.getId(), name).isPresent()) {
        throw new FileAlreadyExistsException(path.toString());
      }

      var inode = inodeTable.createInode(type);
      directoryTable.addEntry(new DirectoryEntry(parentInode.getId(), name, inode.getId()));
      return inode;
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

      var inodeId = resolvePathToInodeId(absPath)
        .orElseThrow(() -> new NoSuchFileException(path.toString()));
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

      if (absSource.isRoot()) {
        throw new IOException("Cannot move root directory");
      }

      var sourceId = resolvePathToInodeId(absSource).orElseThrow(() -> new NoSuchFileException(source.toString()));

      var sourceInode = inodeTable.get(sourceId).orElseThrow();
      var targetParentId = prepareTarget(sourceInode, target, options);
      var targetName = target.toAbsolutePath().getFileName().toString();

      var sourceParent = (BoxPath) absSource.getParent();
      var sourceName = absSource.getFileName().toString();
      var sourceParentId = resolvePathToInodeId(sourceParent).orElseThrow();

      directoryTable.move(sourceParentId, sourceName, targetParentId, targetName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  void copy(BoxPath source, BoxPath target, CopyOption... options) throws IOException {
    lock.writeLock().lock();
    try {
      checkWritable();

      var absSource = (BoxPath) source.toAbsolutePath();

      var sourceInode = resolvePathToInode(absSource)
        .orElseThrow(() -> new NoSuchFileException(source.toString()));
      if (sourceInode.isDirectory()) {
        throw new IOException("Cannot copy directories");
      }

      var targetParentId = prepareTarget(sourceInode, target, options);
      var targetName = target.toAbsolutePath().getFileName().toString();

      var newInode = inodeTable.createInode(Inode.Type.FILE);
      directoryTable.addEntry(new DirectoryEntry(targetParentId, targetName, newInode.getId()));

      var sourceSize = sourceInode.getSize();
      if (sourceSize > 0) {
        var blockSize = containerIO.getBlockSize();
        var blocksNeeded = (int) ((sourceSize + blockSize - 1) / blockSize);

        var newExtents = spaceManager.allocateMultiple(blocksNeeded);
        if (newExtents.isEmpty()) {
          throw new IOException("No space available for copy");
        }

        for (var extent : newExtents) {
          newInode.addExtent(extent);
        }

        var buffer = ByteBuffer.allocate((int) sourceSize);
        readFileData(sourceInode, 0, buffer);
        buffer.flip();

        for (var extent : newInode.getExtents()) {
          var bytesToWrite = (int) Math.min(buffer.remaining(), extent.sizeInBytes(blockSize));
          if (bytesToWrite > 0) {
            containerIO.writeToExtent(extent, 0, buffer);
          }
        }

        newInode.setSize(sourceSize);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Validates the target path for copy/move operations.
   * Checks that target parent exists and is a directory.
   * If the target exists and REPLACE_EXISTING is set, validates type compatibility and deletes it.
   */
  private Long prepareTarget(Inode sourceInode, BoxPath target, CopyOption... options) throws IOException {
    var absTarget = (BoxPath) target.toAbsolutePath();

    var targetParent = (BoxPath) absTarget.getParent();
    if (targetParent == null) {
      throw new IOException("Invalid target path");
    }

    var targetParentId = resolvePathToInodeId(targetParent)
      .orElseThrow(() -> new NoSuchFileException(targetParent.toString()));

    var targetParentInode = inodeTable.get(targetParentId).orElseThrow();
    if (!targetParentInode.isDirectory()) {
      throw new NotDirectoryException(targetParent.toString());
    }

    var targetName = absTarget.getFileName().toString();
    var replaceExisting = Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING);
    var targetIdOpt = directoryTable.lookup(targetParentId, targetName);

    if (targetIdOpt.isPresent()) {
      if (!replaceExisting) {
        throw new FileAlreadyExistsException(target.toString());
      }
      var targetInode = inodeTable.get(targetIdOpt.get()).orElseThrow();
      // POSIX semantics: cannot replace file with directory or vice versa
      if (sourceInode.isFile() && targetInode.isDirectory()) {
        throw new IOException("Cannot replace directory with file");
      }
      if (sourceInode.isDirectory() && targetInode.isFile()) {
        throw new IOException("Cannot replace file with directory");
      }
      delete(absTarget);
    }

    return targetParentId;
  }

  void checkAccess(BoxPath path, AccessMode... ignored) throws IOException {
    lock.readLock().lock();
    try {
      resolvePathToInode((BoxPath) path.toAbsolutePath())
        .orElseThrow(() -> new NoSuchFileException(path.toString()));
    } finally {
      lock.readLock().unlock();
    }
  }

  BasicFileAttributes readAttributes(BoxPath path) throws IOException {
    lock.readLock().lock();
    try {
      var inode = resolvePathToInode((BoxPath) path.toAbsolutePath())
        .orElseThrow(() -> new NoSuchFileException(path.toString()));
      return new BoxBasicFileAttributes(inode);
    } finally {
      lock.readLock().unlock();
    }
  }

  List<Path> listDirectory(BoxPath path) throws IOException {
    lock.readLock().lock();
    try {
      var absPath = (BoxPath) path.toAbsolutePath();
      var inode = resolvePathToInode(absPath)
        .orElseThrow(() -> new NoSuchFileException(path.toString()));

      if (!inode.isDirectory()) {
        throw new NotDirectoryException(path.toString());
      }

      var entries = directoryTable.listChildren(inode.getId());
      var children = new ArrayList<Path>();

      for (var entry : entries) {
        children.add(absPath.resolve(entry.name()));
      }

      return children;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Helper class to map an extent to its byte range within a file.
   */
  private record ExtentRange(Extent extent, long startByte, long endByte) {
    boolean notContains(long position) {
      return position < startByte || position >= endByte;
    }

    long offsetAt(long position) {
      return position - startByte;
    }

    long bytesAvailableFrom(long position) {
      return endByte - position;
    }
  }

  /**
   * Pre-computes byte ranges for all extents in a file.
   */
  private List<ExtentRange> computeExtentRanges(List<Extent> extents, int blockSize) {
    var ranges = new ArrayList<ExtentRange>();
    var cumulativeBytes = 0L;
    for (var extent : extents) {
      var endByte = cumulativeBytes + extent.sizeInBytes(blockSize);
      ranges.add(new ExtentRange(extent, cumulativeBytes, endByte));
      cumulativeBytes = endByte;
    }
    return ranges;
  }

  int readFileData(Inode inode, long position, ByteBuffer dest) throws IOException {
    lock.readLock().lock();
    try {
      if (position >= inode.getSize()) {
        return -1;
      }

      var blockSize = containerIO.getBlockSize();
      var extentRanges = computeExtentRanges(inode.getExtents(), blockSize);
      var totalBytesRead = 0;
      var currentPosition = position;
      var remainingInFile = inode.getSize() - position;

      for (var range : extentRanges) {
        if (range.notContains(currentPosition)) {
          continue;
        }

        var offsetInExtent = range.offsetAt(currentPosition);
        var bytesAvailableInExtent = range.bytesAvailableFrom(currentPosition);
        var bytesToRead = (int) Math.min(
          Math.min(bytesAvailableInExtent, dest.remaining()),
          remainingInFile - totalBytesRead
        );

        if (bytesToRead <= 0) {
          break;
        }

        var readBuffer = ByteBuffer.allocate(bytesToRead);
        var bytesRead = containerIO.readFromExtent(range.extent(), offsetInExtent, readBuffer);

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

      var extentRanges = computeExtentRanges(inode.getExtents(), blockSize);
      var totalBytesWritten = 0;
      var currentPosition = position;

      for (var range : extentRanges) {
        if (range.notContains(currentPosition)) {
          continue;
        }

        var offsetInExtent = range.offsetAt(currentPosition);
        var bytesAvailableInExtent = range.bytesAvailableFrom(currentPosition);
        var bytesForThisExtent = (int) Math.min(bytesAvailableInExtent, src.remaining());

        if (bytesForThisExtent <= 0) {
          break;
        }

        var written = containerIO.writeToExtent(range.extent(), offsetInExtent, src);
        totalBytesWritten += written;
        currentPosition += written;

        if (!src.hasRemaining()) {
          break;
        }
      }

      if (endPosition > inode.getSize()) {
        inode.setSize(endPosition);
      }

      inode.touch();

      return totalBytesWritten;
    } finally {
      lock.writeLock().unlock();
    }
  }

  void truncateFile(Inode inode, long newSize) {
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
      inode.touch();

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
    return false;
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
  public @NotNull Path getPath(@NotNull String first, String... more) {
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
    var colonIndex = syntaxAndPattern.indexOf(':');
    if (colonIndex <= 0 || colonIndex == syntaxAndPattern.length() - 1) {
      throw new IllegalArgumentException("Invalid pattern: " + syntaxAndPattern);
    }

    var syntax = syntaxAndPattern.substring(0, colonIndex);
    var pattern = syntaxAndPattern.substring(colonIndex + 1);

    Pattern regex;
    if (syntax.equalsIgnoreCase("glob")) {
      regex = Pattern.compile(globToRegex(pattern));
    } else if (syntax.equalsIgnoreCase("regex")) {
      try {
        regex = Pattern.compile(pattern);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid regex pattern", e);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported syntax: " + syntax);
    }

    return path -> regex.matcher(path.toString()).matches();
  }

  private static final String REGEX_META_CHARS = ".^$+{[]|()";
  private static final String GLOB_META_CHARS = "\\*?[{";

  private static boolean isRegexMeta(char c) {
    return REGEX_META_CHARS.indexOf(c) != -1;
  }

  private static boolean isGlobMeta(char c) {
    return GLOB_META_CHARS.indexOf(c) != -1;
  }

  private static char nextChar(String glob, int i) {
    return i < glob.length() ? glob.charAt(i) : 0;
  }

  // Glob-to-regex conversion adapted from OpenJDK's sun.nio.fs.Globs
  // Source: https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/sun/nio/fs/Globs.java
  private static String globToRegex(String globPattern) {
    var inGroup = false;
    var regex = new StringBuilder("^");

    var i = 0;
    while (i < globPattern.length()) {
      var c = globPattern.charAt(i++);
      switch (c) {
        case '\\' -> {
          if (i == globPattern.length()) {
            throw new PatternSyntaxException("No character to escape", globPattern, i - 1);
          }
          var next = globPattern.charAt(i++);
          if (isGlobMeta(next) || isRegexMeta(next)) {
            regex.append('\\');
          }
          regex.append(next);
        }
        case '[' -> {
          regex.append("[[^/]&&[");
          if (nextChar(globPattern, i) == '^') {
            regex.append("\\^");
            i++;
          } else {
            if (nextChar(globPattern, i) == '!') {
              regex.append('^');
              i++;
            }
            if (nextChar(globPattern, i) == '-') {
              regex.append('-');
              i++;
            }
          }
          var hasRangeStart = false;
          var last = (char) 0;
          while (i < globPattern.length()) {
            c = globPattern.charAt(i++);
            if (c == ']') {
              break;
            }
            if (c == '/') {
              throw new PatternSyntaxException("Explicit 'name separator' in class", globPattern, i - 1);
            }
            if (c == '\\' || c == '[' || c == '&' && nextChar(globPattern, i) == '&') {
              regex.append('\\');
            }
            regex.append(c);
            if (c == '-') {
              if (!hasRangeStart) {
                throw new PatternSyntaxException("Invalid range", globPattern, i - 1);
              }
              c = nextChar(globPattern, i++);
              if (c == 0 || c == ']') {
                break;
              }
              if (c < last) {
                throw new PatternSyntaxException("Invalid range", globPattern, i - 3);
              }
              regex.append(c);
              hasRangeStart = false;
            } else {
              hasRangeStart = true;
              last = c;
            }
          }
          if (c != ']') {
            throw new PatternSyntaxException("Missing ']", globPattern, i - 1);
          }
          regex.append("]]");
        }
        case '{' -> {
          if (inGroup) {
            throw new PatternSyntaxException("Cannot nest groups", globPattern, i - 1);
          }
          regex.append("(?:(?:");
          inGroup = true;
        }
        case '}' -> {
          if (inGroup) {
            regex.append("))");
            inGroup = false;
          } else {
            regex.append('}');
          }
        }
        case ',' -> {
          if (inGroup) {
            regex.append(")|(?:");
          } else {
            regex.append(',');
          }
        }
        case '*' -> {
          if (nextChar(globPattern, i) == '*') {
            regex.append(".*");
            i++;
          } else {
            regex.append("[^/]*");
          }
        }
        case '?' -> regex.append("[^/]");
        default -> {
          if (isRegexMeta(c)) {
            regex.append('\\');
          }
          regex.append(c);
        }
      }
    }

    if (inGroup) {
      throw new PatternSyntaxException("Missing '}", globPattern, i - 1);
    }

    return regex.append('$').toString();
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

  void checkOpen() {
    if (!open) {
      throw new ClosedFileSystemException();
    }
  }

  void checkWritable() {
    checkOpen();
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
}
