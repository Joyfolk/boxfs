package org.test.boxfs.internal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Low-level I/O operations for the container file.
 * Provides block-level read/write operations.
 */
public class ContainerIO implements Closeable {

  private final FileChannel channel;
  private final Superblock superblock;
  private boolean closed;

  private ContainerIO(FileChannel channel, Superblock superblock) {
    this.channel = channel;
    this.superblock = superblock;
    this.closed = false;
  }

  public static ContainerIO create(Path path, int blockSize, long totalBlocks) throws IOException {
    var channel = FileChannel.open(path,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE);

    var superblock = new Superblock(blockSize, totalBlocks);

    var buffer = ByteBuffer.wrap(superblock.serialize());
    channel.position(0);
    while (buffer.hasRemaining()) {
      //noinspection ResultOfMethodCallIgnored
      channel.write(buffer);
    }

    // Pre-allocate the file by writing a byte at the end
    var totalSize = (long) blockSize + (totalBlocks * blockSize);
    channel.position(totalSize - 1);
    var endByte = ByteBuffer.wrap(new byte[]{0});
    while (endByte.hasRemaining()) {
      //noinspection ResultOfMethodCallIgnored
      channel.write(endByte);
    }

    return new ContainerIO(channel, superblock);
  }

  public static ContainerIO open(Path path) throws IOException {
    var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

    var buffer = ByteBuffer.allocate(Superblock.HEADER_SIZE);
    channel.position(0);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) == -1) {
        throw new IOException("Unexpected end of file while reading superblock");
      }
    }

    var superblock = Superblock.deserialize(buffer.array());
    return new ContainerIO(channel, superblock);
  }

  public Superblock getSuperblock() {
    return superblock;
  }

  public int getBlockSize() {
    return superblock.getBlockSize();
  }

  public long getTotalBlocks() {
    return superblock.getTotalBlocks();
  }

  public byte[] readBlocks(long startBlock, int blockCount) throws IOException {
    checkNotClosed();
    validateBlockNumber(startBlock);
    validateBlockNumber(startBlock + blockCount - 1);

    var totalSize = blockCount * superblock.getBlockSize();
    var data = new byte[totalSize];
    var buffer = ByteBuffer.wrap(data);
    var offset = superblock.blockOffset(startBlock);

    channel.position(offset);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) == -1) {
        break;
      }
    }

    return data;
  }

  public void writeBlocks(long startBlock, byte[] data) throws IOException {
    checkNotClosed();
    validateBlockNumber(startBlock);

    var blockSize = superblock.getBlockSize();
    var blockCount = (data.length + blockSize - 1) / blockSize;
    validateBlockNumber(startBlock + blockCount - 1);

    var paddedSize = blockCount * blockSize;
    var paddedData = data.length < paddedSize
      ? copyWithPadding(data, paddedSize)
      : data;

    var buffer = ByteBuffer.wrap(paddedData);
    var offset = superblock.blockOffset(startBlock);
    channel.position(offset);
    while (buffer.hasRemaining()) {
      //noinspection ResultOfMethodCallIgnored
      channel.write(buffer);
    }
  }

  private byte[] copyWithPadding(byte[] data, int paddedSize) {
    var padded = new byte[paddedSize];
    System.arraycopy(data, 0, padded, 0, data.length);
    return padded;
  }

  public int readFromExtent(Extent extent, long offsetInExtent, ByteBuffer dest) throws IOException {
    checkNotClosed();

    var blockSize = superblock.getBlockSize();
    var extentSize = extent.sizeInBytes(blockSize);

    if (offsetInExtent >= extentSize) {
      return -1;
    }

    var availableBytes = extentSize - offsetInExtent;
    var bytesToRead = (int) Math.min(availableBytes, dest.remaining());

    var absoluteOffset = superblock.blockOffset(extent.startBlock()) + offsetInExtent;

    var originalLimit = dest.limit();
    dest.limit(dest.position() + bytesToRead);
    var bytesRead = 0;
    var currentOffset = absoluteOffset;
    while (dest.hasRemaining()) {
      var n = channel.read(dest, currentOffset);
      if (n == -1) {
        break;
      }
      bytesRead += n;
      currentOffset += n;
    }
    dest.limit(originalLimit);

    return bytesRead > 0 ? bytesRead : -1;
  }

  public int writeToExtent(Extent extent, long offsetInExtent, ByteBuffer src) throws IOException {
    checkNotClosed();

    var blockSize = superblock.getBlockSize();
    var extentSize = extent.sizeInBytes(blockSize);

    if (offsetInExtent >= extentSize) {
      return 0;
    }

    var availableBytes = extentSize - offsetInExtent;
    var bytesToWrite = (int) Math.min(availableBytes, src.remaining());

    var absoluteOffset = superblock.blockOffset(extent.startBlock()) + offsetInExtent;

    var originalLimit = src.limit();
    src.limit(src.position() + bytesToWrite);
    var bytesWritten = 0;
    var currentOffset = absoluteOffset;
    while (src.hasRemaining()) {
      var n = channel.write(src, currentOffset);
      bytesWritten += n;
      currentOffset += n;
    }
    src.limit(originalLimit);

    return bytesWritten;
  }

  public void writeSuperblock() throws IOException {
    checkNotClosed();
    var buffer = ByteBuffer.wrap(superblock.serialize());
    channel.position(0);
    while (buffer.hasRemaining()) {
      //noinspection ResultOfMethodCallIgnored
      channel.write(buffer);
    }
  }

  public void sync() throws IOException {
    checkNotClosed();
    channel.force(true);
  }

  private void validateBlockNumber(long blockNumber) {
    if (blockNumber < 0 || blockNumber >= superblock.getTotalBlocks()) {
      throw new IllegalArgumentException("Block number out of range: " + blockNumber);
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("ContainerIO is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      channel.close();
    }
  }
}
