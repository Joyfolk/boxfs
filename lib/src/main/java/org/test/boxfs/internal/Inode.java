package org.test.boxfs.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a file or directory inode with metadata and extent list.
 */
public class Inode {

    public enum Type {
        FILE((byte) 0),
        DIRECTORY((byte) 1);

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static Type fromCode(byte code) {
            return switch (code) {
                case 0 -> FILE;
                case 1 -> DIRECTORY;
                default -> throw new IllegalArgumentException("Unknown inode type code: " + code);
            };
        }
    }

    private final long id;
    private final Type type;
    private long size;
    private final List<Extent> extents;
    private long creationTime;
    private long lastModifiedTime;
    private long lastAccessTime;

    public Inode(long id, Type type) {
        this(id, type, 0, new ArrayList<>(), System.currentTimeMillis());
    }

    public Inode(long id, Type type, long size, List<Extent> extents, long creationTime) {
        this(id, type, size, extents, creationTime, creationTime, creationTime);
    }

    public Inode(long id, Type type, long size, List<Extent> extents,
                 long creationTime, long lastModifiedTime, long lastAccessTime) {
        if (id < 0) {
            throw new IllegalArgumentException("id must be non-negative");
        }
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        this.id = id;
        this.type = type;
        this.size = size;
        this.extents = new ArrayList<>(extents);
        this.creationTime = creationTime;
        this.lastModifiedTime = lastModifiedTime;
        this.lastAccessTime = lastAccessTime;
    }

    public long getId() {
        return id;
    }

    public Type getType() {
        return type;
    }

    public boolean isFile() {
        return type == Type.FILE;
    }

    public boolean isDirectory() {
        return type == Type.DIRECTORY;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        this.size = size;
    }

    public List<Extent> getExtents() {
        return Collections.unmodifiableList(extents);
    }

    public void addExtent(Extent extent) {
        extents.add(extent);
    }

    public void setExtents(List<Extent> newExtents) {
        extents.clear();
        extents.addAll(newExtents);
    }

    public void clearExtents() {
        extents.clear();
    }

    /**
     * Returns the total number of allocated blocks.
     */
    public long getAllocatedBlocks() {
        return extents.stream().mapToLong(Extent::blockCount).sum();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public void setTimes(Long lastModifiedTime, Long lastAccessTime, Long creationTime) {
        if (lastModifiedTime != null) {
            this.lastModifiedTime = lastModifiedTime;
        }
        if (lastAccessTime != null) {
            this.lastAccessTime = lastAccessTime;
        }
        if (creationTime != null) {
            this.creationTime = creationTime;
        }
    }

    public void touch() {
        var now = System.currentTimeMillis();
        this.lastModifiedTime = now;
        this.lastAccessTime = now;
    }
}
