package org.test.boxfs.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;

/**
 * In-memory table mapping inode IDs to Inode objects.
 * Provides O(1) inode lookup.
 */
public class InodeTable {

    public static final long ROOT_INODE_ID = 0L;

    private final HashMap<Long, Inode> inodes = new HashMap<>();
    private long nextInodeId = ROOT_INODE_ID;

    /**
     * Creates and registers the root directory inode.
     */
    public Inode createRootInode() {
        if (inodes.containsKey(ROOT_INODE_ID)) {
            throw new IllegalStateException("Root inode already exists");
        }
        var root = new Inode(ROOT_INODE_ID, Inode.Type.DIRECTORY);
        inodes.put(ROOT_INODE_ID, root);
        nextInodeId = ROOT_INODE_ID + 1;
        return root;
    }

    /**
     * Creates a new inode with an auto-assigned ID.
     */
    public Inode createInode(Inode.Type type) {
        var id = nextInodeId++;
        var inode = new Inode(id, type);
        inodes.put(id, inode);
        return inode;
    }

    /**
     * Registers an inode (used during deserialization).
     */
    public void registerInode(Inode inode) {
        inodes.put(inode.getId(), inode);
        if (inode.getId() >= nextInodeId) {
            nextInodeId = inode.getId() + 1;
        }
    }

    /**
     * Gets an inode by ID.
     */
    public Optional<Inode> get(long id) {
        return Optional.ofNullable(inodes.get(id));
    }

    /**
     * Gets the root inode.
     */
    public Optional<Inode> getRoot() {
        return get(ROOT_INODE_ID);
    }

    /**
     * Removes an inode.
     */
    public void remove(long id) {
        if (id == ROOT_INODE_ID) {
            throw new IllegalArgumentException("Cannot remove root inode");
        }
        inodes.remove(id);
    }

    /**
     * Returns all inodes.
     */
    public Collection<Inode> getAllInodes() {
        return inodes.values();
    }

    /**
     * Returns the number of inodes.
     */
    public int size() {
        return inodes.size();
    }

    /**
     * Checks if an inode exists.
     */
    public boolean contains(long id) {
        return inodes.containsKey(id);
    }

    /**
     * Clears all inodes.
     */
    public void clear() {
        inodes.clear();
        nextInodeId = ROOT_INODE_ID;
    }
}
