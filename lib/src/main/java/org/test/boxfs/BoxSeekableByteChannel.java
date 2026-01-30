package org.test.boxfs;

import org.test.boxfs.internal.Inode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.Set;

/**
 * SeekableByteChannel implementation for BoxFS files.
 * Supports read, write, position, and truncate operations.
 *
 * <p>Limitation with TRUNCATE_EXISTING: When opening a channel with
 * {@link StandardOpenOption#TRUNCATE_EXISTING} (e.g., via {@code Files.write(path, data)}),
 * there is a brief window between the truncation and subsequent write operations where
 * concurrent readers may observe an empty file. This occurs because truncation and write
 * are separate operations, each acquiring and releasing the filesystem lock independently.
 */
public class BoxSeekableByteChannel implements SeekableByteChannel {

  private final BoxFileSystem fileSystem;
  private final boolean readable;
  private final boolean writable;
  private final boolean append;
  private final Inode inode;
  private long position;
  private volatile boolean open;

  BoxSeekableByteChannel(BoxFileSystem fileSystem, BoxPath path, Set<? extends OpenOption> options)
    throws IOException {
    this.fileSystem = fileSystem;
    BoxPath path1 = (BoxPath) path.toAbsolutePath();
    this.position = 0;
    this.open = true;

    // Parse options
    var read = options.isEmpty() || options.contains(StandardOpenOption.READ);
    var write = options.contains(StandardOpenOption.WRITE);
    var create = options.contains(StandardOpenOption.CREATE);
    var createNew = options.contains(StandardOpenOption.CREATE_NEW);
    var truncateExisting = options.contains(StandardOpenOption.TRUNCATE_EXISTING);
    this.append = options.contains(StandardOpenOption.APPEND);

    if (write || append || truncateExisting) {
      write = true;
    }

    this.readable = read;
    this.writable = write;

    // Get or create the file
    var existingInode = fileSystem.resolvePathToInode(path1);

    if (existingInode.isPresent()) {
      if (createNew) {
        throw new FileAlreadyExistsException(path.toString());
      }
      this.inode = existingInode.get();

      if (!inode.isFile()) {
        throw new IOException("Not a regular file: " + path);
      }

      if (truncateExisting) {
        fileSystem.truncateFile(inode, 0);
      }
    } else {
      if (!create && !createNew) {
        throw new NoSuchFileException(path.toString());
      }
      if (!writable) {
        throw new NoSuchFileException(path.toString());
      }
      this.inode = fileSystem.createEntry(path1, Inode.Type.FILE);
    }

    if (append) {
      this.position = inode.getSize();
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkOpen();
    if (!readable) {
      throw new NonWritableChannelException();
    }

    var bytesRead = fileSystem.readFileData(inode, position, dst);
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    checkOpen();
    if (!writable) {
      throw new NonWritableChannelException();
    }

    if (append) {
      position = inode.getSize();
    }

    var bytesWritten = fileSystem.writeFileData(inode, position, src);
    position += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long position() throws IOException {
    checkOpen();
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    checkOpen();
    if (newPosition < 0) {
      throw new IllegalArgumentException("Position cannot be negative");
    }
    this.position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    checkOpen();
    return inode.getSize();
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    checkOpen();
    if (!writable) {
      throw new NonWritableChannelException();
    }
    if (size < 0) {
      throw new IllegalArgumentException("Size cannot be negative");
    }

    fileSystem.truncateFile(inode, size);

    if (position > size) {
      position = size;
    }

    return this;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    // Durability is only guaranteed after explicit FileSystem.close() or sync() call.
    open = false;
  }

  private void checkOpen() throws IOException {
    if (!open) {
      throw new ClosedChannelException();
    }
    fileSystem.checkOpen();
  }
}
