package org.test.boxfs.internal;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetadataSerializerTest {

    @Test
    void serializeAndDeserializeEmpty() throws IOException {
        var inodeTable = new InodeTable();
        var directoryTable = new DirectoryTable();
        var spaceManager = new SpaceManager(100);

        inodeTable.createRootInode();
        spaceManager.initializeNew(1);

        var data = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager);
        assertNotNull(data);
        assertTrue(data.length > 0);

        // Deserialize into new objects
        var restoredInodes = new InodeTable();
        var restoredDirs = new DirectoryTable();
        var restoredSpace = new SpaceManager(100);

        MetadataSerializer.deserialize(data, restoredInodes, restoredDirs, restoredSpace);

        assertEquals(1, restoredInodes.size());
        assertTrue(restoredInodes.getRoot().isPresent());
        assertEquals(0, restoredDirs.size());
    }

    @Test
    void serializeAndDeserializeWithFiles() throws IOException {
        var inodeTable = new InodeTable();
        var directoryTable = new DirectoryTable();
        var spaceManager = new SpaceManager(100);

        var root = inodeTable.createRootInode();
        spaceManager.initializeNew(1);

        // Create a file
        var file = inodeTable.createInode(Inode.Type.FILE);
        file.setSize(1024);
        file.addExtent(new Extent(10, 1));

        directoryTable.addEntry(new DirectoryEntry(root.getId(), "test.txt", file.getId()));

        var data = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager);

        // Deserialize
        var restoredInodes = new InodeTable();
        var restoredDirs = new DirectoryTable();
        var restoredSpace = new SpaceManager(100);

        MetadataSerializer.deserialize(data, restoredInodes, restoredDirs, restoredSpace);

        assertEquals(2, restoredInodes.size());
        assertEquals(1, restoredDirs.size());

        // Verify file was restored
        var restoredFile = restoredInodes.get(file.getId());
        assertTrue(restoredFile.isPresent());
        assertEquals(1024, restoredFile.get().getSize());
        assertEquals(1, restoredFile.get().getExtents().size());
    }

    @Test
    void serializeAndDeserializeWithDirectories() throws IOException {
        var inodeTable = new InodeTable();
        var directoryTable = new DirectoryTable();
        var spaceManager = new SpaceManager(100);

        var root = inodeTable.createRootInode();
        spaceManager.initializeNew(1);

        // Create directory structure
        var dir1 = inodeTable.createInode(Inode.Type.DIRECTORY);
        directoryTable.addEntry(new DirectoryEntry(root.getId(), "dir1", dir1.getId()));

        var dir2 = inodeTable.createInode(Inode.Type.DIRECTORY);
        directoryTable.addEntry(new DirectoryEntry(dir1.getId(), "dir2", dir2.getId()));

        var file = inodeTable.createInode(Inode.Type.FILE);
        file.setSize(2048);
        directoryTable.addEntry(new DirectoryEntry(dir2.getId(), "file.txt", file.getId()));

        var data = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager);

        // Deserialize
        var restoredInodes = new InodeTable();
        var restoredDirs = new DirectoryTable();
        var restoredSpace = new SpaceManager(100);

        MetadataSerializer.deserialize(data, restoredInodes, restoredDirs, restoredSpace);

        assertEquals(4, restoredInodes.size());
        assertEquals(3, restoredDirs.size());

        // Verify path resolution works
        var childId = restoredDirs.lookup(root.getId(), "dir1");
        assertTrue(childId.isPresent());
        assertEquals(dir1.getId(), childId.get());
    }

    @Test
    void serializedSizeDependsOnFreeListSize() throws IOException {
        // This test demonstrates why persistMetadata needs a loop:
        // the serialized size changes when the free list changes

        var inodeTable = new InodeTable();
        var directoryTable = new DirectoryTable();
        var spaceManager = new SpaceManager(100);

        inodeTable.createRootInode();

        // Set a free list with 1 extent
        spaceManager.setFreeExtents(List.of(new Extent(1, 99)));
        var sizeWith1Extent = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager).length;

        // Set a free list with 3 extents (fragmented)
        spaceManager.setFreeExtents(List.of(
                new Extent(1, 30),
                new Extent(40, 30),
                new Extent(80, 20)
        ));
        var sizeWith3Extents = MetadataSerializer.serialize(inodeTable, directoryTable, spaceManager).length;

        // More extents = larger serialized size
        assertTrue(sizeWith3Extents > sizeWith1Extent,
                "Serialized size should increase with more free extents");

        // Each extent adds 12 bytes (8 for startBlock + 4 for blockCount)
        assertEquals(24, sizeWith3Extents - sizeWith1Extent,
                "Each additional extent should add 12 bytes");
    }
}
