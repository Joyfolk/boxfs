# BoxFS - Single-Container File System

BoxFS is a Java library that emulates a typical file system API, storing all files and folders within a single container file. It implements the Java NIO `FileSystemProvider` SPI, allowing seamless integration with standard `java.nio.file.Files` operations.

## Features

- **All required operations**: create, write, read, append, delete, rename, move
- **Extent-based storage**: Files stored as contiguous block ranges for efficient I/O
- **Minimal RAM usage**: Only metadata is kept in memory; file content streams directly to/from disk
- **Minimal disk operations**: Metadata persisted only on sync/close, not on every operation
- **Thread-safe**: `ReentrantReadWriteLock` allows concurrent reads with exclusive writes
- **No external dependencies**: Only JUnit for testing

## Design Overview

### Assumptions and Simplifications

The task allowed introducing simplifications. The following choices were made:

| Decision                           | Rationale                                             |
|------------------------------------|-------------------------------------------------------|
| No compression                     | Explicitly required by task                           |
| No encryption                      | Out of scope for core functionality                   |
| No symlinks/hardlinks              | Adds complexity without core value                    |
| Timestamps always return epoch (0) | Simplifies implementation; can be added later         |
| No permissions model               | Not required for core file operations                 |
| Case-sensitive paths               | Default Java behavior, avoids platform differences    |
| Crash consistency not guaranteed   | Data safe only after explicit `close()` or `sync()`   |
| Fixed container size               | Capacity set at creation; no dynamic growth           |
| Serialized writes                  | Filesystem-level lock sufficient; no per-file locking |

### Core Architecture

The system uses a block-based extent allocation strategy. The file system treats its own metadata as a special "internal file," allowing it to grow and move dynamically without fixed-size constraints.

**Key Principles:**
- **Metadata in RAM**: Directory structure and file index loaded at startup for O(1) path resolution
- **Extent-based storage**: Files stored as lists of extents `(startBlock, blockCount)`, minimizing metadata overhead and enabling contiguous disk I/O
- **Unified allocation**: Single "Space Manager" handles free space for both user data and system metadata

### On-Disk Format

The container file is divided into a fixed-size Superblock and a dynamic area of uniform blocks.

#### Superblock (first block, offset 0)

The only fixed-location structure, used to bootstrap the system. Takes one full block:

```
Offset  Size  Field
------  ----  -----
0       4     Magic number (0x424F5846 = "BOXF")
4       4     Version (1)
8       4     Block size in bytes (default: 4096)
12      8     Total block count
20      4     Metadata extent count
24      N×12  Metadata extents (startBlock:8, blockCount:4 each)
...     ...   Zero padding to block size
```

#### Metadata Region

Stored in blocks pointed to by the superblock's metadata extents. Serialized on close/sync:

```
Section 1: Inode Table
  [4 bytes]  Inode count
  [per inode]
    [8 bytes]  Inode ID
    [1 byte]   Type (0 = file, 1 = directory)
    [8 bytes]  Size in bytes
    [4 bytes]  Extent count
    [per extent]
      [8 bytes]  Start block
      [4 bytes]  Block count

Section 2: Directory Entries
  [4 bytes]  Entry count
  [per entry]
    [8 bytes]  Parent inode ID
    [8 bytes]  Child inode ID
    [2 bytes]  Name length
    [N bytes]  Name (UTF-8, max 255 bytes)

Section 3: Free Space Map
  [4 bytes]  Free extent count
  [per extent]
    [8 bytes]  Start block
    [4 bytes]  Block count
```

#### Data Blocks

File content is stored in data blocks (default 4KB). Each file's inode contains an extent list mapping logical file offsets to physical block locations.

### Key Operations

**Initialization:**
1. Read superblock from offset 0
2. Follow metadata extents to read serialized metadata
3. Deserialize inode table, directory entries, and free list into RAM

**Space Management:**
- First-fit allocation from free list
- Deleted extents returned to free list
- Adjacent free extents coalesced automatically

**File I/O:**
- Logical position translated to physical offset: `physicalOffset = blockIndex × blockSize + offsetInBlock`
- Data streamed via ByteBuffer (no full-file buffering)

## Usage

The library provides two ways to interact with containers:

### Option 1: Java NIO API (Full Control)

The task asked to "design and implement a library to emulate the typical file system API." This was interpreted as implementing the standard Java NIO FileSystemProvider SPI, which allows using familiar `java.nio.file.Files` methods:

```java
import java.nio.file.*;
import java.net.URI;
import java.util.Map;

// Create a new container (1MB capacity)
URI uri = URI.create("box:/path/to/container.box");
Map<String, Object> env = Map.of("create", "true", "totalBlocks", 256L);

try (FileSystem fs = FileSystems.newFileSystem(uri, env)) {
    // Use standard Files API
    Path dir = fs.getPath("/documents");
    Files.createDirectory(dir);

    Path file = dir.resolve("hello.txt");
    Files.write(file, "Hello, World!".getBytes());

    byte[] content = Files.readAllBytes(file);
    System.out.println(new String(content));

    Files.delete(file);
}

// Open existing container
try (FileSystem fs = FileSystems.newFileSystem(uri, Map.of())) {
    // ... use the filesystem
}
```

#### More NIO Examples

**Append to a file:**
```java
Files.write(file, "more data".getBytes(), StandardOpenOption.APPEND);
```

**Random access with SeekableByteChannel:**
```java
try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
    channel.position(100);  // Seek to byte 100
    ByteBuffer buffer = ByteBuffer.allocate(50);
    channel.read(buffer);   // Read 50 bytes from position 100
}
```

**List directory contents:**
```java
try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
    for (Path entry : stream) {
        System.out.println(entry.getFileName());
    }
}
```

**Check file attributes:**
```java
if (Files.exists(file)) {
    long size = Files.size(file);
    boolean isDir = Files.isDirectory(file);
}
```

### Option 2: BoxFs Facade

Since the task mentioned "design" an API (which could imply a custom interface), the library also provides a simplified facade for common operations:

```java
import org.test.boxfs.BoxFs;

// Create a new container
try (BoxFs fs = BoxFs.create(Path.of("container.box"))) {
    // Write using streams (memory-efficient for large files)
    try (OutputStream out = fs.openWrite("/documents/hello.txt")) {
        out.write("Hello, World!".getBytes());
    }

    // Read using streams
    try (InputStream in = fs.openRead("/documents/hello.txt")) {
        byte[] content = in.readAllBytes();
    }

    // Directory operations
    fs.createDirectory("/backup");
    List<String> entries = fs.listDirectory("/documents");

    // File operations
    fs.renameFile("/documents/hello.txt", "/documents/greeting.txt");
    fs.moveFile("/documents/greeting.txt", "/backup/greeting.txt");
    fs.deleteFile("/backup/greeting.txt");
}

// Open existing container
try (BoxFs fs = BoxFs.open(Path.of("container.box"))) {
    // ... use the filesystem
}
```

#### More Facade Examples

**Append to a file:**
```java
try (OutputStream out = fs.openAppend("/log.txt")) {
    out.write("New log entry\n".getBytes());
}
```

**Query file properties:**
```java
if (fs.exists("/data/config.json")) {
    long size = fs.size("/data/config.json");
    boolean isFile = fs.isFile("/data/config.json");
    boolean isDir = fs.isDirectory("/data");
}
```

**Create nested directories:**
```java
fs.createDirectories("/a/b/c/d");  // Creates all parent directories
```

**Custom container capacity:**
```java
// Create container with 10MB capacity (2560 blocks of 4KB)
try (BoxFs fs = BoxFs.create(Path.of("large.box"), 2560)) {
    // ...
}
```

## Build and Test

### Requirements

- Java 21 or later
- Gradle 8.x 

### Build

```bash
./gradlew lib:build
```

### Run All Tests

```bash
./gradlew lib:test
```

### IntelliJ IDEA

Open the project in IntelliJ IDEA. The Gradle project will be imported automatically.

## Performance and Scalability Analysis

### CPU

- **Path resolution**: O(d) where d is path depth; each level is O(1) hash map lookup
- **File read/write**: O(n) where n is data size; no overhead beyond copying bytes
- **Metadata operations**: O(d) due to path resolution; actual mutation is O(1)
- **Startup**: O(m) where m is the total metadata size (number of files/directories)

### RAM

- **Metadata only**: O(n + e + f) where n is files/directories, e is file extents, f is free extents
- **No content buffering**: File data streams directly between container and user buffers

### Disk

- **Metadata I/O**: Only on startup (read) and close/sync (write)
- **Data I/O**: Single sequential read/write per extent; no amplification
- **Space overhead**: Depends on file count and average file size; higher for many small files

### Scalability Limits

| Dimension          | Limiting Factor                              |
|--------------------|----------------------------------------------|
| File count         | Available RAM (metadata kept in memory)      |
| File size          | Container size                               |
| Container size     | Host filesystem max file size                |
| Concurrent readers | Multiple (read lock is shared)               |
| Concurrent writers | One at a time (filesystem-level write lock)  |

Design limits (e.g., 32-bit counts in serialization format, HashMap capacity) exist but exceed practical hardware constraints.
