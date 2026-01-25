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
    void rejectInvalidBlockSize() {
        assertThrows(IllegalArgumentException.class, () -> new Superblock(1000, 256));
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
        assertEquals(0, sb.getMetadataExtents().get(0).startBlock());
    }

    @Test
    void serializeAndDeserialize() throws IOException {
        var original = new Superblock(4096, 256);
        original.addMetadataExtent(new Extent(0, 2));
        original.addMetadataExtent(new Extent(5, 3));

        var data = original.serialize();
        assertEquals(Superblock.SUPERBLOCK_SIZE, data.length);

        var restored = Superblock.deserialize(data);
        assertEquals(original.getBlockSize(), restored.getBlockSize());
        assertEquals(original.getTotalBlocks(), restored.getTotalBlocks());
        assertEquals(2, restored.getMetadataExtents().size());
    }

    @Test
    void blockOffsetCalculation() {
        var sb = new Superblock(4096, 256);
        // Block 0 starts after superblock (512 bytes)
        assertEquals(512, sb.blockOffset(0));
        // Block 1 starts at 512 + 4096
        assertEquals(512 + 4096, sb.blockOffset(1));
    }

    @Test
    void rejectTooManyMetadataExtents() {
        var sb = new Superblock(4096, 256);
        for (int i = 0; i < Superblock.MAX_METADATA_EXTENTS; i++) {
            sb.addMetadataExtent(new Extent(i * 10, 1));
        }
        assertThrows(IllegalStateException.class, () -> sb.addMetadataExtent(new Extent(100, 1)));
    }
}
