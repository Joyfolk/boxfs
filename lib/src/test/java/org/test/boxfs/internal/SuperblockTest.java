package org.test.boxfs.internal;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SuperblockTest {

    @Test
    void createWithDefaults() {
        var sb = new Superblock(4096, 256);
        assertEquals(4096, sb.getBlockSize());
        assertEquals(256, sb.getTotalBlocks());
        assertTrue(sb.getMetadataExtents().isEmpty());
    }

    @Test
    void rejectNonPowerOfTwoBlockSize() {
        assertThrows(IllegalArgumentException.class, () -> new Superblock(1000, 256));
    }

    @Test
    void rejectBlockSizeBelowMinimum() {
        assertThrows(IllegalArgumentException.class, () -> new Superblock(256, 256));
    }

    @Test
    void acceptMinimumBlockSize() {
        var sb = new Superblock(512, 256);
        assertEquals(512, sb.getBlockSize());
    }

    @Test
    void rejectZeroTotalBlocks() {
        assertThrows(IllegalArgumentException.class, () -> new Superblock(4096, 0));
    }

    @Test
    void addMetadataExtent() {
        var sb = new Superblock(4096, 256);
        sb.addMetadataExtent(new Extent(0, 2));
        assertEquals(1, sb.getMetadataExtents().size());
        assertEquals(0, sb.getMetadataExtents().getFirst().startBlock());
    }

    @Test
    void serializeAndDeserialize() throws IOException {
        var original = new Superblock(4096, 256);
        original.addMetadataExtent(new Extent(0, 2));
        original.addMetadataExtent(new Extent(5, 3));

        var data = original.serialize();
        assertEquals(4096, data.length);

        var restored = Superblock.deserialize(data);
        assertEquals(original.getBlockSize(), restored.getBlockSize());
        assertEquals(original.getTotalBlocks(), restored.getTotalBlocks());
        assertEquals(2, restored.getMetadataExtents().size());
    }

    @Test
    void blockOffsetCalculation() {
        var sb = new Superblock(4096, 256);
        assertEquals(4096, sb.blockOffset(0));
        assertEquals(8192, sb.blockOffset(1));
    }

    @Test
    void rejectTooManyMetadataExtents() {
        var sb = new Superblock(4096, 256);
        int maxExtents = sb.getMaxMetadataExtents();
        for (int i = 0; i < maxExtents; i++) {
            sb.addMetadataExtent(new Extent(i * 10, 1));
        }
        assertThrows(IllegalStateException.class, () -> sb.addMetadataExtent(new Extent(100, 1)));
    }
}
