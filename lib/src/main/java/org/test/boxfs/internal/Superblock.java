package org.test.boxfs.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The superblock stored at sector 0 of the container.
 * Contains bootstrap information for the file system.
 */
public class Superblock {

    public static final int MIN_BLOCK_SIZE = 512;
    public static final int MAGIC = 0x424F5846; // "BOXF"
    public static final int VERSION = 1;
    public static final int DEFAULT_BLOCK_SIZE = 4096;

    // Header: magic(4) + version(4) + blockSize(4) + totalBlocks(8) + extentCount(4) = 24 bytes
    private static final int HEADER_FIXED_SIZE = 24;
    // Each extent: startBlock(8) + blockCount(4) = 12 bytes
    private static final int EXTENT_SIZE = 12;

    private final int blockSize;
    private final long totalBlocks;
    private final List<Extent> metadataExtents = new ArrayList<>();

  public Superblock(int blockSize, long totalBlocks) {
        if (blockSize < MIN_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be at least " + MIN_BLOCK_SIZE);
        }
        if ((blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("blockSize must be a power of 2");
        }
        if (totalBlocks <= 0) {
            throw new IllegalArgumentException("totalBlocks must be positive");
        }
        this.blockSize = blockSize;
        this.totalBlocks = totalBlocks;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public long getTotalBlocks() {
        return totalBlocks;
    }

    public List<Extent> getMetadataExtents() {
        return Collections.unmodifiableList(metadataExtents);
    }

    public int getMaxMetadataExtents() {
        return (blockSize - HEADER_FIXED_SIZE) / EXTENT_SIZE;
    }

    public void setMetadataExtents(List<Extent> extents) {
        int max = getMaxMetadataExtents();
        if (extents.size() > max) {
            throw new IllegalArgumentException("Too many metadata extents: " + extents.size() + " (max " + max + ")");
        }
        metadataExtents.clear();
        metadataExtents.addAll(extents);
    }

    public void addMetadataExtent(Extent extent) {
        if (metadataExtents.size() >= getMaxMetadataExtents()) {
            throw new IllegalStateException("Cannot add more metadata extents");
        }
        metadataExtents.add(extent);
    }

    /**
     * Serializes the superblock to a byte buffer.
     */
    public byte[] serialize() {
        var buffer = ByteBuffer.allocate(blockSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        buffer.putInt(MAGIC);
        buffer.putInt(VERSION);
        buffer.putInt(blockSize);
        buffer.putLong(totalBlocks);
        buffer.putInt(metadataExtents.size());

        for (var extent : metadataExtents) {
            buffer.putLong(extent.startBlock());
            buffer.putInt(extent.blockCount());
        }

        while (buffer.position() < blockSize) {
            buffer.put((byte) 0);
        }

        return buffer.array();
    }

    /**
     * Deserializes a superblock from a byte array.
     */
    public static Superblock deserialize(byte[] data) throws IOException {
        if (data.length < HEADER_FIXED_SIZE) {
            throw new IOException("Superblock data too short");
        }

        var buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);

        var magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new IOException("Invalid magic number: " + Integer.toHexString(magic));
        }

        var version = buffer.getInt();
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        var blockSize = buffer.getInt();
        var totalBlocks = buffer.getLong();
        var extentCount = buffer.getInt();

        var superblock = new Superblock(blockSize, totalBlocks);
        int maxExtents = superblock.getMaxMetadataExtents();

        if (extentCount < 0 || extentCount > maxExtents) {
            throw new IOException("Invalid metadata extent count: " + extentCount + " (max " + maxExtents + ")");
        }

        for (var i = 0; i < extentCount; i++) {
            var startBlock = buffer.getLong();
            var blockCount = buffer.getInt();
            superblock.addMetadataExtent(new Extent(startBlock, blockCount));
        }

        return superblock;
    }

    /**
     * Returns the byte offset for a given block number.
     */
    public long blockOffset(long blockNumber) {
        return blockSize + (blockNumber * blockSize);
    }
}
