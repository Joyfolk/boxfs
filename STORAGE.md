# BoxFS Storage Format

This document describes the on-disk binary format for BoxFS containers.

## Container Layout

```
+------------------+  Offset 0
|    Superblock    |  blockSize bytes (1 block)
+------------------+  Offset blockSize
|     Block 0      |  blockSize bytes
+------------------+
|     Block 1      |
+------------------+
|       ...        |
+------------------+
|   Block N-1      |
+------------------+
```

Total container size: `(1 + totalBlocks) × blockSize` bytes

The superblock occupies exactly one block at the beginning of the container.

## Byte Order

All multi-byte values are stored in **big-endian** format.

## Superblock (blockSize bytes)

Located at offset 0, occupies one full block. The header fields are within the first 512 bytes.

| Offset | Size | Type   | Description                               |
|--------|------|--------|-------------------------------------------|
| 0      | 4    | uint32 | Magic number: `0x424F5846` ("BOXF")       |
| 4      | 4    | uint32 | Version: `1`                              |
| 8      | 4    | uint32 | Block size in bytes (min 512, power of 2) |
| 12     | 8    | uint64 | Total number of blocks                    |
| 20     | 4    | uint32 | Metadata extent count                     |
| 24     | 12×N | extent | Metadata extents array                    |
| ...    | ...  | zeros  | Padding to blockSize                      |

### Extent Structure (12 bytes)

| Offset | Size | Type   | Description          |
|--------|------|--------|----------------------|
| 0      | 8    | uint64 | Start block number   |
| 8      | 4    | uint32 | Block count          |

## Metadata

Metadata is stored in blocks referenced by the superblock's metadata extents.
Serialized sequentially as: Inodes → Directory Entries → Free Extents.

### Metadata Header

| Offset | Size | Type   | Description          |
|--------|------|--------|----------------------|
| 0      | 4    | uint32 | Inode count          |

### Inode Entry

Repeated `inodeCount` times:

| Offset | Size | Type   | Description                       |
|--------|------|--------|-----------------------------------|
| 0      | 8    | uint64 | Inode ID                          |
| 8      | 1    | uint8  | Type: `0` = file, `1` = directory |
| 9      | 8    | uint64 | File size in bytes                |
| 17     | 8    | int64  | Creation time (millis since epoch)|
| 25     | 8    | int64  | Last modified time (millis)       |
| 33     | 8    | int64  | Last access time (millis)         |
| 41     | 4    | uint32 | Extent count for this inode       |
| 45     | 12×N | extent | Data extents array                |

Root directory always has inode ID `0`.

### Directory Entries Section

| Offset | Size | Type   | Description              |
|--------|------|--------|--------------------------|
| 0      | 4    | uint32 | Directory entry count    |

Followed by entries:

| Offset | Size | Type   | Description              |
|--------|------|--------|--------------------------|
| 0      | 8    | uint64 | Parent inode ID          |
| 8      | 8    | uint64 | Child inode ID           |
| 16     | 2    | uint16 | Name length in bytes     |
| 18     | N    | UTF-8  | File/directory name      |

### Free Extents Section

| Offset | Size | Type   | Description          |
|--------|------|--------|----------------------|
| 0      | 4    | uint32 | Free extent count    |
| 4      | 12×N | extent | Free extents array   |

## Data Storage

File data is stored in blocks referenced by the inode's extent list.
Extents are allocated using a first-fit strategy and coalesced on free.

### Example: File with 10KB of data (blockSize = 4096)

```
Inode:
  id: 1
  type: FILE (0)
  size: 10240
  extents: [(startBlock: 5, blockCount: 3)]

Data layout:
  Block 5: bytes 0-4095
  Block 6: bytes 4096-8191
  Block 7: bytes 8192-10239 (+ padding)
```

## Default Values

| Parameter    | Default Value |
|--------------|---------------|
| Block size   | 4096 bytes    |
| Total blocks | 256           |

## Constraints

- Block size must be a power of 2
- Minimum block size: 512 bytes
- Maximum metadata extents: `(blockSize - 24) / 12` (dynamic based on block size)
- Maximum file name length: 65535 bytes (uint16 limit)
- Inode IDs are sequential, starting from 0 (root)
- Superblock occupies exactly one block
