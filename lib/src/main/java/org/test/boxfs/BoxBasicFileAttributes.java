package org.test.boxfs;

import org.test.boxfs.internal.Inode;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * BasicFileAttributes implementation for BoxFS.
 * Timestamps are minimal (returns epoch) as per design.
 */
public class BoxBasicFileAttributes implements BasicFileAttributes {

    private static final FileTime EPOCH = FileTime.fromMillis(0);

    private final Inode inode;

    BoxBasicFileAttributes(Inode inode) {
        this.inode = inode;
    }

    @Override
    public FileTime lastModifiedTime() {
        return EPOCH;
    }

    @Override
    public FileTime lastAccessTime() {
        return EPOCH;
    }

    @Override
    public FileTime creationTime() {
        return EPOCH;
    }

    @Override
    public boolean isRegularFile() {
        return inode.isFile();
    }

    @Override
    public boolean isDirectory() {
        return inode.isDirectory();
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return inode.getSize();
    }

    @Override
    public Object fileKey() {
        return inode.getId();
    }
}
