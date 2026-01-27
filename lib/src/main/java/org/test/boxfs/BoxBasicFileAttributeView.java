package org.test.boxfs;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * BasicFileAttributeView implementation for BoxFS.
 */
class BoxBasicFileAttributeView implements BasicFileAttributeView {

  private final BoxFileSystem fs;
  private final BoxPath path;

  BoxBasicFileAttributeView(BoxFileSystem fs, BoxPath path) {
    this.fs = fs;
    this.path = path;
  }

  @Override
  public String name() {
    return "basic";
  }

  @Override
  public BasicFileAttributes readAttributes() throws IOException {
    return fs.readAttributes(path);
  }

  @Override
  public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
    var inode = fs.resolvePathToInode(path)
      .orElseThrow(() -> new NoSuchFileException(path.toString()));
    inode.setTimes(
      lastModifiedTime != null ? lastModifiedTime.toMillis() : null,
      lastAccessTime != null ? lastAccessTime.toMillis() : null,
      createTime != null ? createTime.toMillis() : null
    );
  }
}
