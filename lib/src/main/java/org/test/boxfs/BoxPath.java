package org.test.boxfs;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/**
 * Immutable path implementation for BoxFS.
 */
public class BoxPath implements Path {

    private final BoxFileSystem fileSystem;
    private final String path;
    private final boolean absolute;
    private final List<String> components;

    BoxPath(BoxFileSystem fileSystem, String path) {
        this.fileSystem = fileSystem;
        this.path = normalize(path);
        this.absolute = this.path.startsWith("/");
        this.components = parseComponents(this.path);
    }

    private BoxPath(BoxFileSystem fileSystem, String path, boolean absolute, List<String> components) {
        this.fileSystem = fileSystem;
        this.path = path;
        this.absolute = absolute;
        this.components = components;
    }

    private static String normalize(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }

        var isAbsolute = path.startsWith("/");
        var parts = path.split("/");
        var normalized = new ArrayList<String>();

        for (var part : parts) {
            if (part.isEmpty() || ".".equals(part)) {
                continue;
            }
            if ("..".equals(part)) {
                if (!normalized.isEmpty()) {
                    normalized.removeLast();
                }
            } else {
                normalized.add(part);
            }
        }

        if (normalized.isEmpty()) {
            return isAbsolute ? "/" : "";
        }

        var result = String.join("/", normalized);
        return isAbsolute ? "/" + result : result;
    }

    private static List<String> parseComponents(String path) {
        if (path.isEmpty() || "/".equals(path)) {
            return List.of();
        }

        var withoutLeadingSlash = path.startsWith("/") ? path.substring(1) : path;
        if (withoutLeadingSlash.isEmpty()) {
            return List.of();
        }

        return List.of(withoutLeadingSlash.split("/"));
    }

    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return absolute;
    }

    @Override
    public Path getRoot() {
        return absolute ? new BoxPath(fileSystem, "/") : null;
    }

    @Override
    public Path getFileName() {
        if (components.isEmpty()) {
            return null;
        }
        return new BoxPath(fileSystem, components.getLast(), false, List.of(components.getLast()));
    }

    @Override
    public Path getParent() {
        if (components.isEmpty()) {
            return null;
        }
        if (components.size() == 1) {
            return absolute ? getRoot() : null;
        }

        var parentComponents = components.subList(0, components.size() - 1);
        var parentPath = String.join("/", parentComponents);
        if (absolute) {
            parentPath = "/" + parentPath;
        }
        return new BoxPath(fileSystem, parentPath, absolute, new ArrayList<>(parentComponents));
    }

    @Override
    public int getNameCount() {
        return components.size();
    }

    @Override
    public Path getName(int index) {
        if (index < 0 || index >= components.size()) {
            throw new IllegalArgumentException("Index out of bounds: " + index);
        }
        return new BoxPath(fileSystem, components.get(index), false, List.of(components.get(index)));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        if (beginIndex < 0 || endIndex > components.size() || beginIndex >= endIndex) {
            throw new IllegalArgumentException("Invalid subpath range");
        }

        var subComponents = components.subList(beginIndex, endIndex);
        var subPath = String.join("/", subComponents);
        return new BoxPath(fileSystem, subPath, false, new ArrayList<>(subComponents));
    }

    @Override
    public boolean startsWith(Path other) {
        if (!(other instanceof BoxPath boxOther)) {
            return false;
        }
        if (this.absolute != boxOther.absolute) {
            return false;
        }
        if (boxOther.components.size() > this.components.size()) {
            return false;
        }
        for (var i = 0; i < boxOther.components.size(); i++) {
            if (!this.components.get(i).equals(boxOther.components.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean endsWith(Path other) {
        if (!(other instanceof BoxPath boxOther)) {
            return false;
        }
        if (boxOther.absolute && !this.absolute) {
            return false;
        }
        if (boxOther.absolute && boxOther.components.size() != this.components.size()) {
            return false;
        }
        if (boxOther.components.size() > this.components.size()) {
            return false;
        }

        var offset = this.components.size() - boxOther.components.size();
        for (var i = 0; i < boxOther.components.size(); i++) {
            if (!this.components.get(offset + i).equals(boxOther.components.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Path normalize() {
        // Already normalized in constructor
        return this;
    }

    @Override
    public Path resolve(Path other) {
        if (other instanceof BoxPath boxOther) {
            if (boxOther.isAbsolute()) {
                return boxOther;
            }
            if (boxOther.path.isEmpty()) {
                return this;
            }
            var resolvedPath = this.path.isEmpty() || this.path.equals("/")
                    ? (absolute ? "/" : "") + boxOther.path
                    : this.path + "/" + boxOther.path;
            return new BoxPath(fileSystem, resolvedPath);
        }
        return resolve(other.toString());
    }

    @Override
    public Path resolve(String other) {
        return resolve(new BoxPath(fileSystem, other));
    }

    @Override
    public Path relativize(Path other) {
        if (!(other instanceof BoxPath boxOther)) {
            throw new IllegalArgumentException("Path is not a BoxPath");
        }
        if (this.absolute != boxOther.absolute) {
            throw new IllegalArgumentException("Cannot relativize paths of different types");
        }

        // Find common prefix
        var commonLength = 0;
        var minLength = Math.min(this.components.size(), boxOther.components.size());
        while (commonLength < minLength &&
               this.components.get(commonLength).equals(boxOther.components.get(commonLength))) {
            commonLength++;
        }

        // Build relative path
        var relativeComponents = new ArrayList<String>();

        // Add ".." for each remaining component in this path
        for (var i = commonLength; i < this.components.size(); i++) {
            relativeComponents.add("..");
        }

        // Add remaining components from other path
        for (var i = commonLength; i < boxOther.components.size(); i++) {
            relativeComponents.add(boxOther.components.get(i));
        }

        if (relativeComponents.isEmpty()) {
            return new BoxPath(fileSystem, "");
        }

        var relativePath = String.join("/", relativeComponents);
        return new BoxPath(fileSystem, relativePath);
    }

    @Override
    public URI toUri() {
        var containerPath = fileSystem.getContainerPath().toString();
        return URI.create("box:" + containerPath + "!" + toAbsolutePath());
    }

    @Override
    public Path toAbsolutePath() {
        if (absolute) {
            return this;
        }
        return new BoxPath(fileSystem, "/" + path);
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        var abs = toAbsolutePath();
        // Verify the path exists
        fileSystem.checkAccess(this);
        return abs;
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
        throw new UnsupportedOperationException("Watch service not supported");
    }

    @Override
    public int compareTo(Path other) {
        if (!(other instanceof BoxPath boxOther)) {
            throw new ClassCastException("Cannot compare to non-BoxPath");
        }
        return this.path.compareTo(boxOther.path);
    }

    @Override
    public Iterator<Path> iterator() {
        return new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < components.size();
            }

            @Override
            public Path next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return getName(index++);
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BoxPath other)) return false;
        return this.fileSystem.equals(other.fileSystem) && this.path.equals(other.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSystem, path);
    }

    @Override
    public String toString() {
        return path.isEmpty() ? (absolute ? "/" : ".") : path;
    }

    /**
     * Returns the path components.
     */
    List<String> getComponents() {
        return components;
    }

    /**
     * Returns the raw path string.
     */
    String getPathString() {
        return path;
    }

    /**
     * Checks if this path represents the root directory.
     */
    boolean isRoot() {
        return absolute && components.isEmpty();
    }
}
