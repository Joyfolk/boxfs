package org.test.boxfs.internal;

/**
 * Represents a contiguous range of blocks in the container.
 *
 * @param startBlock the first block index of this extent
 * @param blockCount the number of contiguous blocks
 */
public record Extent(long startBlock, int blockCount) {

    public Extent {
        if (startBlock < 0) {
            throw new IllegalArgumentException("startBlock must be non-negative");
        }
        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive");
        }
    }

    /**
     * Returns the block index immediately after this extent.
     */
    public long endBlock() {
        return startBlock + blockCount;
    }

    /**
     * Checks if this extent is adjacent to another extent (can be merged).
     */
    public boolean isAdjacentTo(Extent other) {
        return this.endBlock() == other.startBlock || other.endBlock() == this.startBlock;
    }

    /**
     * Merges this extent with an adjacent extent.
     * @throws IllegalArgumentException if extents are not adjacent
     */
    public Extent mergeWith(Extent other) {
        if (this.endBlock() == other.startBlock) {
            return new Extent(this.startBlock, this.blockCount + other.blockCount);
        } else if (other.endBlock() == this.startBlock) {
            return new Extent(other.startBlock, this.blockCount + other.blockCount);
        }
        throw new IllegalArgumentException("Extents are not adjacent");
    }

    /**
     * Returns the size in bytes for a given block size.
     */
    public long sizeInBytes(int blockSize) {
        return (long) blockCount * blockSize;
    }
}
