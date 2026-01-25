package org.test.boxfs.internal;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExtentTest {

    @Test
    void createValidExtent() {
        var extent = new Extent(10, 5);
        assertEquals(10, extent.startBlock());
        assertEquals(5, extent.blockCount());
    }

    @Test
    void endBlockCalculation() {
        var extent = new Extent(10, 5);
        assertEquals(15, extent.endBlock());
    }

    @Test
    void rejectNegativeStartBlock() {
        assertThrows(IllegalArgumentException.class, () -> new Extent(-1, 5));
    }

    @Test
    void rejectZeroBlockCount() {
        assertThrows(IllegalArgumentException.class, () -> new Extent(0, 0));
    }

    @Test
    void rejectNegativeBlockCount() {
        assertThrows(IllegalArgumentException.class, () -> new Extent(0, -1));
    }

    @Test
    void adjacentExtents() {
        var a = new Extent(0, 5);
        var b = new Extent(5, 3);
        assertTrue(a.isAdjacentTo(b));
        assertTrue(b.isAdjacentTo(a));
    }

    @Test
    void nonAdjacentExtents() {
        var a = new Extent(0, 5);
        var b = new Extent(10, 3);
        assertFalse(a.isAdjacentTo(b));
    }

    @Test
    void mergeAdjacentExtents() {
        var a = new Extent(0, 5);
        var b = new Extent(5, 3);
        var merged = a.mergeWith(b);
        assertEquals(0, merged.startBlock());
        assertEquals(8, merged.blockCount());
    }

    @Test
    void sizeInBytes() {
        var extent = new Extent(0, 4);
        assertEquals(16384, extent.sizeInBytes(4096));
    }
}
