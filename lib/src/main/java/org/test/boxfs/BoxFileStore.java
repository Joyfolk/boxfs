package org.test.boxfs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

/**
 * FileStore implementation for BoxFS.
 * Provides storage information about the container.
 */
public class BoxFileStore extends FileStore {

  private final BoxFileSystem fileSystem;

  BoxFileStore(BoxFileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  @Override
  public String name() {
    return fileSystem.getContainerPath().getFileName().toString();
  }

  @Override
  public String type() {
    return "boxfs";
  }

  @Override
  public boolean isReadOnly() {
    return fileSystem.isReadOnly();
  }

  @Override
  public long getTotalSpace() {
    return fileSystem.getTotalBlocks() * fileSystem.getBlockSize();
  }

  @Override
  public long getUsableSpace() {
    return fileSystem.getFreeBlocks() * fileSystem.getBlockSize();
  }

  @Override
  public long getUnallocatedSpace() {
    return getUsableSpace();
  }

  @Override
  public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
    return type == BasicFileAttributeView.class;
  }

  @Override
  public boolean supportsFileAttributeView(String name) {
    return "basic".equals(name);
  }

  @Override
  public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
    return null;
  }

  @Override
  public Object getAttribute(String attribute) {
    return switch (attribute) {
      case "totalSpace" -> getTotalSpace();
      case "usableSpace" -> getUsableSpace();
      case "unallocatedSpace" -> getUnallocatedSpace();
      default -> throw new UnsupportedOperationException("Unknown attribute: " + attribute);
    };
  }
}
