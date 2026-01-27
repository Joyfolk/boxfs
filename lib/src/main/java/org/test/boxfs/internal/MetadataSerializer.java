package org.test.boxfs.internal;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Serializes and deserializes file system metadata (inodes, directory entries, free space)
 * to/from binary format.
 */
public class MetadataSerializer {

    private MetadataSerializer() {}

    /**
     * Serializes all metadata to a byte array.
     */
    public static byte[] serialize(InodeTable inodeTable, DirectoryTable directoryTable,
                                   SpaceManager spaceManager) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);

        // Write inodes
        var inodes = inodeTable.getAllInodes();
        dos.writeInt(inodes.size());
        for (var inode : inodes) {
            writeInode(dos, inode);
        }

        // Write directory entries
        var entries = directoryTable.getAllEntries();
        dos.writeInt(entries.size());
        for (var entry : entries) {
            writeDirectoryEntry(dos, entry);
        }

        // Write free extents
        var freeExtents = spaceManager.getFreeExtents();
        dos.writeInt(freeExtents.size());
        for (var extent : freeExtents) {
            writeExtent(dos, extent);
        }

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Deserializes metadata from a byte array.
     */
    public static void deserialize(byte[] data, InodeTable inodeTable,
                                   DirectoryTable directoryTable, SpaceManager spaceManager)
            throws IOException {
        var bais = new ByteArrayInputStream(data);
        var dis = new DataInputStream(bais);

        // Clear existing data
        inodeTable.clear();
        directoryTable.clear();

        // Read inodes
        var inodeCount = dis.readInt();
        for (var i = 0; i < inodeCount; i++) {
            var inode = readInode(dis);
            inodeTable.registerInode(inode);
        }

        // Read directory entries
        var entryCount = dis.readInt();
        for (var i = 0; i < entryCount; i++) {
            var entry = readDirectoryEntry(dis);
            directoryTable.addEntry(entry);
        }

        // Read free extents
        var freeExtentCount = dis.readInt();
        var freeExtents = new ArrayList<Extent>(freeExtentCount);
        for (var i = 0; i < freeExtentCount; i++) {
            freeExtents.add(readExtent(dis));
        }
        spaceManager.setFreeExtents(freeExtents);
    }

    private static void writeInode(DataOutputStream dos, Inode inode) throws IOException {
        dos.writeLong(inode.getId());
        dos.writeByte(inode.getType().code());
        dos.writeLong(inode.getSize());
        dos.writeLong(inode.getCreationTime());
        dos.writeLong(inode.getLastModifiedTime());
        dos.writeLong(inode.getLastAccessTime());

        var extents = inode.getExtents();
        dos.writeInt(extents.size());
        for (var extent : extents) {
            writeExtent(dos, extent);
        }
    }

    private static Inode readInode(DataInputStream dis) throws IOException {
        var id = dis.readLong();
        var type = Inode.Type.fromCode(dis.readByte());
        var size = dis.readLong();
        var creationTime = dis.readLong();
        var lastModifiedTime = dis.readLong();
        var lastAccessTime = dis.readLong();

        var extentCount = dis.readInt();
        var extents = new ArrayList<Extent>(extentCount);
        for (var i = 0; i < extentCount; i++) {
            extents.add(readExtent(dis));
        }

        return new Inode(id, type, size, extents, creationTime, lastModifiedTime, lastAccessTime);
    }

    private static void writeDirectoryEntry(DataOutputStream dos, DirectoryEntry entry)
            throws IOException {
        dos.writeLong(entry.parentId());
        dos.writeLong(entry.childId());

        var nameBytes = entry.name().getBytes(StandardCharsets.UTF_8);
        dos.writeShort(nameBytes.length);
        dos.write(nameBytes);
    }

    private static DirectoryEntry readDirectoryEntry(DataInputStream dis) throws IOException {
        var parentId = dis.readLong();
        var childId = dis.readLong();

        var nameLength = dis.readUnsignedShort();
        var nameBytes = new byte[nameLength];
        dis.readFully(nameBytes);
        var name = new String(nameBytes, StandardCharsets.UTF_8);

        return new DirectoryEntry(parentId, name, childId);
    }

    private static void writeExtent(DataOutputStream dos, Extent extent) throws IOException {
        dos.writeLong(extent.startBlock());
        dos.writeInt(extent.blockCount());
    }

    private static Extent readExtent(DataInputStream dis) throws IOException {
        var startBlock = dis.readLong();
        var blockCount = dis.readInt();
        return new Extent(startBlock, blockCount);
    }
}
