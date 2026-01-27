package org.test.boxfs;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * DirectoryStream implementation for BoxFS directories.
 * All entries are buffered upfront (loaded and filtered in constructor),
 * so the iterator may return elements even after close() is called.
 * This could be seen as a minor contract violation, but the spec allows
 * "one or more elements" to be returned after close due to buffering.
 */
public class BoxDirectoryStream implements DirectoryStream<Path> {

  private final List<Path> children;
  private boolean open = true;
  private boolean iteratorReturned = false;

  BoxDirectoryStream(BoxFileSystem fileSystem, BoxPath directory, Filter<? super Path> filter) throws IOException {
    this.children = fileSystem.listDirectory(directory).stream()
        .filter(path -> {
          try {
            return filter == null || filter.accept(path);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .toList();
  }

  @Override
  public @NotNull Iterator<Path> iterator() {
    if (!open) {
      throw new IllegalStateException("Directory stream is closed");
    }
    if (iteratorReturned) {
      throw new IllegalStateException("Iterator already returned");
    }
    iteratorReturned = true;
    return children.iterator();
  }

  @Override
  public void close() {
    open = false;
  }
}
