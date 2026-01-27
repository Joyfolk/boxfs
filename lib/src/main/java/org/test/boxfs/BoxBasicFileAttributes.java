package org.test.boxfs;

import org.test.boxfs.internal.Inode;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * BasicFileAttributes implementation for BoxFS.
 */
public class BoxBasicFileAttributes implements BasicFileAttributes {

  private final Inode inode;

  BoxBasicFileAttributes(Inode inode) {
    this.inode = inode;
  }

  @Override
  public FileTime lastModifiedTime() {
    return FileTime.fromMillis(inode.getLastModifiedTime());
  }

  @Override
  public FileTime lastAccessTime() {
    return FileTime.fromMillis(inode.getLastAccessTime());
  }

  @Override
  public FileTime creationTime() {
    return FileTime.fromMillis(inode.getCreationTime());
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
