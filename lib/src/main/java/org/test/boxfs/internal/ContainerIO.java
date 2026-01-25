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

    /**
     * Creates a new container file.
     */
    public static ContainerIO create(Path path, int blockSize, long totalBlocks) throws IOException {
        var channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        var superblock = new Superblock(blockSize, totalBlocks);

        var buffer = ByteBuffer.wrap(superblock.serialize());
        channel.position(0);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        var totalSize = (long) blockSize + (totalBlocks * blockSize);
        channel.position(totalSize - 1);
        channel.write(ByteBuffer.wrap(new byte[]{0}));

        return new ContainerIO(channel, superblock);
    }

    /**
     * Opens an existing container file.
     */
    public static ContainerIO open(Path path, boolean readOnly) throws IOException {
        var channel = readOnly
                ? FileChannel.open(path, StandardOpenOption.READ)
                : FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

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

    /**
     * Reads a single block.
     */
    public byte[] readBlock(long blockNumber) throws IOException {
        checkNotClosed();
        validateBlockNumber(blockNumber);

        var data = new byte[superblock.getBlockSize()];
        var buffer = ByteBuffer.wrap(data);
        var offset = superblock.blockOffset(blockNumber);

        channel.position(offset);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                break;
            }
        }

        return data;
    }

    /**
     * Writes a single block.
     */
    public void writeBlock(long blockNumber, byte[] data) throws IOException {
        checkNotClosed();
        validateBlockNumber(blockNumber);

        if (data.length > superblock.getBlockSize()) {
            throw new IllegalArgumentException("Data exceeds block size");
        }

        ByteBuffer buffer;
        if (data.length < superblock.getBlockSize()) {
            var padded = new byte[superblock.getBlockSize()];
            System.arraycopy(data, 0, padded, 0, data.length);
            buffer = ByteBuffer.wrap(padded);
        } else {
            buffer = ByteBuffer.wrap(data);
        }

        var offset = superblock.blockOffset(blockNumber);
        channel.position(offset);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * Reads multiple contiguous blocks.
     */
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

    /**
     * Writes multiple contiguous blocks.
     */
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
            channel.write(buffer);
        }
    }

    private byte[] copyWithPadding(byte[] data, int paddedSize) {
        var padded = new byte[paddedSize];
        System.arraycopy(data, 0, padded, 0, data.length);
        return padded;
    }

    /**
     * Reads data from an extent at a specific offset within the extent.
     */
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
        channel.position(absoluteOffset);

        var originalLimit = dest.limit();
        dest.limit(dest.position() + bytesToRead);
        var bytesRead = 0;
        while (dest.hasRemaining()) {
            var n = channel.read(dest);
            if (n == -1) {
                break;
            }
            bytesRead += n;
        }
        dest.limit(originalLimit);

        return bytesRead > 0 ? bytesRead : -1;
    }

    /**
     * Writes data to an extent at a specific offset within the extent.
     */
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
        channel.position(absoluteOffset);

        var originalLimit = src.limit();
        src.limit(src.position() + bytesToWrite);
        var bytesWritten = 0;
        while (src.hasRemaining()) {
            bytesWritten += channel.write(src);
        }
        src.limit(originalLimit);

        return bytesWritten;
    }

    /**
     * Writes the superblock to disk.
     */
    public void writeSuperblock() throws IOException {
        checkNotClosed();
        var buffer = ByteBuffer.wrap(superblock.serialize());
        channel.position(0);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * Forces any changes to be written to disk.
     */
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

    public boolean isOpen() {
        return !closed && channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            channel.close();
        }
    }
}
