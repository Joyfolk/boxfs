package org.test.boxfs.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SpaceManagerTest {

    private SpaceManager spaceManager;

    @BeforeEach
    void setUp() {
        spaceManager = new SpaceManager(100);
        spaceManager.initializeNew(1); // Reserve 1 block
    }

    @Test
    void initialFreeSpace() {
        assertEquals(99, spaceManager.getTotalFreeBlocks());
        assertEquals(1, spaceManager.getTotalUsedBlocks());
    }

    @Test
    void allocateSingleExtent() {
        var extent = spaceManager.allocate(10);
        assertTrue(extent.isPresent());
        assertEquals(10, extent.get().blockCount());
        assertEquals(89, spaceManager.getTotalFreeBlocks());
    }

    @Test
    void allocateAllSpace() {
        var extent = spaceManager.allocate(99);
        assertTrue(extent.isPresent());
        assertEquals(0, spaceManager.getTotalFreeBlocks());
    }

    @Test
    void allocateTooMuch() {
        var extent = spaceManager.allocate(100);
        assertTrue(extent.isEmpty());
    }

    @Test
    void freeAndCoalesce() {
        // Allocate two adjacent blocks
        var a = spaceManager.allocate(5).orElseThrow();
        var b = spaceManager.allocate(5).orElseThrow();

        // Free them
        spaceManager.free(a);
        spaceManager.free(b);

        // Should be coalesced
        var freeExtents = spaceManager.getFreeExtents();
        assertEquals(1, freeExtents.size());
        assertEquals(99, freeExtents.getFirst().blockCount());
    }

    @Test
    void allocateMultiple() {
        // Allocate in the middle to fragment
        spaceManager.allocate(10); // blocks 1-10
        var middle = spaceManager.allocate(20).orElseThrow(); // blocks 11-30
        spaceManager.allocate(10); // blocks 31-40

        // Free the middle
        spaceManager.free(middle);

        // Now allocate more than the middle allows
        var extents = spaceManager.allocateMultiple(30);
        assertFalse(extents.isEmpty());
        var totalAllocated = extents.stream().mapToInt(Extent::blockCount).sum();
        assertEquals(30, totalAllocated);
    }

    @Test
    void largestFreeExtent() {
        assertEquals(99, spaceManager.getLargestFreeExtent());

        spaceManager.allocate(50);
        assertEquals(49, spaceManager.getLargestFreeExtent());
    }
}
