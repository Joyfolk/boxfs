package org.test.boxfs.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Manages free space in the container using a free extent list.
 * Uses first-fit allocation and coalesces adjacent free extents.
 */
public class SpaceManager {

    private final List<Extent> freeExtents = new ArrayList<>();
    private final long totalBlocks;

    public SpaceManager(long totalBlocks) {
        this.totalBlocks = totalBlocks;
    }

    /**
     * Initializes free space for a new file system.
     * Reserves block 0 for metadata initially.
     */
    public void initializeNew(int reservedBlocks) {
        freeExtents.clear();
        if (reservedBlocks < totalBlocks) {
            freeExtents.add(new Extent(reservedBlocks, (int) (totalBlocks - reservedBlocks)));
        }
    }

    /**
     * Sets the free extent list (used during deserialization).
     */
    public void setFreeExtents(List<Extent> extents) {
        freeExtents.clear();
        freeExtents.addAll(extents);
        sortAndCoalesce();
    }

    /**
     * Returns a copy of the free extent list.
     */
    public List<Extent> getFreeExtents() {
        return new ArrayList<>(freeExtents);
    }

    /**
     * Allocates a contiguous range of blocks using first-fit.
     *
     * @param blockCount number of blocks to allocate
     * @return the allocated extent, or empty if no space available
     */
    public Optional<Extent> allocate(int blockCount) {
        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive");
        }

        // First-fit: find first extent large enough
        for (var i = 0; i < freeExtents.size(); i++) {
            var free = freeExtents.get(i);
            if (free.blockCount() >= blockCount) {
                var allocated = new Extent(free.startBlock(), blockCount);

                if (free.blockCount() == blockCount) {
                    // Exact fit - remove the free extent
                    freeExtents.remove(i);
                } else {
                    // Partial use - shrink the free extent
                    freeExtents.set(i, new Extent(
                            free.startBlock() + blockCount,
                            free.blockCount() - blockCount
                    ));
                }

                return Optional.of(allocated);
            }
        }

        return Optional.empty();
    }

    /**
     * Allocates multiple extents to satisfy a block request.
     * Used when a single contiguous extent is not available.
     *
     * @param blockCount total blocks needed
     * @return list of allocated extents, or empty list if not enough space
     */
    public List<Extent> allocateMultiple(int blockCount) {
        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive");
        }

        // Check total free space first
        var totalFree = getTotalFreeBlocks();
        if (totalFree < blockCount) {
            return List.of();
        }

        var allocated = new ArrayList<Extent>();
        var remaining = blockCount;

        while (remaining > 0 && !freeExtents.isEmpty()) {
            var free = freeExtents.getFirst();
            var toAllocate = Math.min(remaining, free.blockCount());

            allocated.add(new Extent(free.startBlock(), toAllocate));

            if (free.blockCount() == toAllocate) {
                freeExtents.removeFirst();
            } else {
                freeExtents.set(0, new Extent(
                        free.startBlock() + toAllocate,
                        free.blockCount() - toAllocate
                ));
            }

            remaining -= toAllocate;
        }

        if (remaining > 0) {
            // Should not happen if we checked space, but handle gracefully
            // Return blocks to free list
            for (var extent : allocated) {
                free(extent);
            }
            return List.of();
        }

        return allocated;
    }

    /**
     * Frees a previously allocated extent.
     */
    public void free(Extent extent) {
        freeExtents.add(extent);
        sortAndCoalesce();
    }

    /**
     * Frees multiple extents.
     */
    public void freeAll(List<Extent> extents) {
        freeExtents.addAll(extents);
        sortAndCoalesce();
    }

    /**
     * Returns total free blocks.
     */
    public long getTotalFreeBlocks() {
        return freeExtents.stream()
                .mapToLong(Extent::blockCount)
                .sum();
    }

    /**
     * Returns total used blocks.
     */
    public long getTotalUsedBlocks() {
        return totalBlocks - getTotalFreeBlocks();
    }

    /**
     * Returns the largest contiguous free extent size.
     */
    public int getLargestFreeExtent() {
        return freeExtents.stream()
                .mapToInt(Extent::blockCount)
                .max()
                .orElse(0);
    }

    /**
     * Sorts free extents by start block and coalesces adjacent extents.
     */
    private void sortAndCoalesce() {
        if (freeExtents.size() <= 1) {
            return;
        }

        // Sort by start block
        freeExtents.sort(Comparator.comparingLong(Extent::startBlock));

        // Coalesce adjacent extents
        var coalesced = new ArrayList<Extent>();
        var current = freeExtents.getFirst();

        for (var i = 1; i < freeExtents.size(); i++) {
            var next = freeExtents.get(i);
            if (current.endBlock() == next.startBlock()) {
                // Adjacent - merge
                current = current.mergeWith(next);
            } else {
                coalesced.add(current);
                current = next;
            }
        }
        coalesced.add(current);

        freeExtents.clear();
        freeExtents.addAll(coalesced);
    }

    /**
     * Checks if the specified blocks are free.
     */
    public boolean areFree(long startBlock, int blockCount) {
        var endBlock = startBlock + blockCount;
        for (var free : freeExtents) {
            if (free.startBlock() <= startBlock && free.endBlock() >= endBlock) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns total blocks managed.
     */
    public long getTotalBlocks() {
        return totalBlocks;
    }
}
