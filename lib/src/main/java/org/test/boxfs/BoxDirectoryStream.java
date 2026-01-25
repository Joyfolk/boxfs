package org.test.boxfs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * DirectoryStream implementation for BoxFS directories.
 */
public class BoxDirectoryStream implements DirectoryStream<Path> {

    private final BoxFileSystem fileSystem;
    private final BoxPath directory;
    private final Filter<? super Path> filter;
    private final List<Path> children;
    private volatile boolean open;
    private volatile boolean iteratorReturned;

    BoxDirectoryStream(BoxFileSystem fileSystem, BoxPath directory, Filter<? super Path> filter)
            throws IOException {
        this.fileSystem = fileSystem;
        this.directory = directory;
        this.filter = filter;
        this.children = fileSystem.listDirectory(directory);
        this.open = true;
        this.iteratorReturned = false;
    }

    @Override
    public Iterator<Path> iterator() {
        if (!open) {
            throw new IllegalStateException("Directory stream is closed");
        }
        if (iteratorReturned) {
            throw new IllegalStateException("Iterator already returned");
        }
        iteratorReturned = true;

        return new Iterator<>() {
            private final Iterator<Path> delegate = children.iterator();
            private Path nextPath = findNext();

            private Path findNext() {
                while (delegate.hasNext()) {
                    var candidate = delegate.next();
                    try {
                        if (filter == null || filter.accept(candidate)) {
                            return candidate;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                if (!open) {
                    return false;
                }
                return nextPath != null;
            }

            @Override
            public Path next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                var result = nextPath;
                nextPath = findNext();
                return result;
            }
        };
    }

    @Override
    public void close() throws IOException {
        open = false;
    }
}
