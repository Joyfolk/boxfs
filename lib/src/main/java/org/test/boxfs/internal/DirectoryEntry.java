package org.test.boxfs.internal;

/**
 * Represents a directory entry linking a parent directory to a child inode.
 *
 * @param parentId the inode ID of the parent directory
 * @param name the name of the entry within the parent
 * @param childId the inode ID of the child (file or directory)
 */
public record DirectoryEntry(long parentId, String name, long childId) {

    public DirectoryEntry {
        if (parentId < 0) {
            throw new IllegalArgumentException("parentId must be non-negative");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name must not be null or empty");
        }
        if (name.contains("/")) {
            throw new IllegalArgumentException("name must not contain path separator");
        }
        if (childId < 0) {
            throw new IllegalArgumentException("childId must be non-negative");
        }
    }
}
