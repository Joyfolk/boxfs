package org.test.boxfs.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * In-memory directory structure providing O(1) path resolution.
 * Maps (parentId, name) -> childId.
 */
public class DirectoryTable {

    // Primary index: (parentId, name) -> DirectoryEntry
    private final HashMap<Long, HashMap<String, DirectoryEntry>> parentIndex = new HashMap<>();

    // Reverse index: childId -> DirectoryEntry (for finding parent)
    private final HashMap<Long, DirectoryEntry> childIndex = new HashMap<>();

    /**
     * Adds a directory entry.
     */
    public void addEntry(DirectoryEntry entry) {
        parentIndex.computeIfAbsent(entry.parentId(), _ -> new HashMap<>())
                   .put(entry.name(), entry);
        childIndex.put(entry.childId(), entry);
    }

    /**
     * Removes a directory entry by parent ID and name.
     */
    public Optional<DirectoryEntry> removeEntry(long parentId, String name) {
        var children = parentIndex.get(parentId);
        if (children == null) {
            return Optional.empty();
        }
        var entry = children.remove(name);
        if (entry != null) {
            childIndex.remove(entry.childId());
            if (children.isEmpty()) {
                parentIndex.remove(parentId);
            }
        }
        return Optional.ofNullable(entry);
    }

    /**
     * Looks up a child inode ID by parent and name.
     */
    public Optional<Long> lookup(long parentId, String name) {
        var children = parentIndex.get(parentId);
        if (children == null) {
            return Optional.empty();
        }
        var entry = children.get(name);
        return entry != null ? Optional.of(entry.childId()) : Optional.empty();
    }

    /**
     * Gets the directory entry for a child.
     */
    public Optional<DirectoryEntry> getEntryForChild(long childId) {
        return Optional.ofNullable(childIndex.get(childId));
    }

    /**
     * Lists all children of a directory.
     */
    public List<DirectoryEntry> listChildren(long parentId) {
        var children = parentIndex.get(parentId);
        if (children == null) {
            return List.of();
        }
        return new ArrayList<>(children.values());
    }

    /**
     * Checks if a directory has any children.
     */
    public boolean hasChildren(long parentId) {
        var children = parentIndex.get(parentId);
        return children != null && !children.isEmpty();
    }

    /**
     * Returns all directory entries.
     */
    public List<DirectoryEntry> getAllEntries() {
        return new ArrayList<>(childIndex.values());
    }

    /**
     * Returns the number of entries.
     */
    public int size() {
        return childIndex.size();
    }

    /**
     * Clears all entries.
     */
    public void clear() {
        parentIndex.clear();
        childIndex.clear();
    }

    /**
     * Renames a directory entry.
     */
    public void rename(long parentId, String oldName, String newName) {
        removeEntry(parentId, oldName).ifPresent(old ->
            addEntry(new DirectoryEntry(old.parentId(), newName, old.childId()))
        );
    }

    /**
     * Moves an entry to a new parent.
     */
    public void move(long oldParentId, String oldName, long newParentId, String newName) {
        removeEntry(oldParentId, oldName).ifPresent(old ->
            addEntry(new DirectoryEntry(newParentId, newName, old.childId()))
        );
    }
}
