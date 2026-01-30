# Design Overview: Single-Container File System (SCFS)

## 1. Core Architecture

The system utilizes a block-based extent allocation strategy. To ensure flexibility and minimize disk I/O, the file
system treats its own metadata as a special "internal file," allowing it to grow and move dynamically without
fixed-size constraints.

**Key Principles:**

- **Metadata in RAM:** The directory structure and file index are loaded into memory during initialization.
- **Extent-Based Storage:** Instead of tracking individual blocks, files are stored as a list of extents (start_block,
  block_count). This minimizes metadata overhead and enables contiguous disk I/O.
- **Unified Allocation:** A single "Space Manager" handles free space for both user data and system metadata.

## 2. On-Disk Layout

The container file is divided into a fixed-size Superblock and a dynamic area composed of uniform Blocks (e.g., 512B or
4KB).

### 2.1. Superblock (Sector 0)

The only fixed-location structure. It bootstraps the system:

- **Block Size:** The size of a single allocation unit.
- **Metadata Extents:** An array of extents pointing to the physical blocks where the
  "Metadata File" is currently stored. Max count calculated dynamically based on block size.
- **Total Blocks:** The total capacity of the container.

### 2.2. Metadata File (Serialized Index)

When the system closes or syncs, the internal state is serialized and written to the blocks pointed to by the
Superblock's metadata extents. It contains:

- **Free Space Map:** A list of available extents (Free List).
- **Inode Table:** A list of file descriptors. Each Inode contains:
    - ID / Type (File vs. Directory).
    - Size (in bytes).
    - Extent List (Physical locations of the data).
- **Directory Entries:** A map of (Parent_ID, Name) -> Inode_ID. This supports variable-length names.

## 3. Key Operations & Logic

### 3.1. Initialization (Startup)

1. Read the Superblock.
2. Follow the Metadata Extents to read the "Metadata File" bytes.
3. De-serialize the Directory Tree and Free Space Map into RAM objects.

### 3.2. Space Management

- **Allocation:** Uses a "Best Fit" or "First Fit" strategy on the Free List to minimize fragmentation.
- **Block Reuse:** Deleted file extents are returned to the Free List. Contiguous free extents are merged (coalesced) to
  maintain large writable areas.
- **No Compaction:** To minimize disk operations, the container size remains constant or grows; it does not shrink
  unless a manual "defrag" is triggered (out of scope).

### 3.3. File I/O

- **Read/Write:** The API translates logical file positions to physical offsets using the Inode's extent list:
  `PhysicalOffset = BlockIndex × BlockSize`.
- **Streaming:** Data is transferred via ByteBuffer directly between the container and the user, ensuring low RAM usage
  even for multi-gigabyte files.
- **Random Access:** Fully supported via SeekableByteChannel by translating logical positions to extent-relative
  offsets.

## 4. Performance & Scalability Analysis

- **CPU:** Low overhead; path resolution and metadata lookups happen in-memory.
- **RAM:** Proportional to the total number of files/folders (metadata only). File content is never buffered in full.
- **Disk:** Metadata I/O occurs only on startup, shutdown, or fsync. Data I/O is optimized for speed via contiguous
  extents.

## 5. Technical Assumptions (Implementation Targets)

- **Atomicity:** Consistency is guaranteed only on graceful `close()` or `flush()`.
- **Thread Safety:** Handled via a `ReentrantReadWriteLock` at the FileSystem level (many readers, one writer).
- **Naming:** Variable-length names are supported up to 255 bytes.

