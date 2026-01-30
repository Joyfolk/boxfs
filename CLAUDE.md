# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BoxFS is a Single-Container File System (SCFS) implementation in Java. It provides a block-based extent allocation file system stored within a single container file, implementing Java's `SeekableByteChannel` for random access support.

## Build Commands

```bash
# Build the project
./gradlew lib:build

# Run tests
./gradlew lib:test

# Run a single test class
./gradlew lib:test --tests "org.test.boxfs.BoxFsIntegrationTest"

# Run a single test method
./gradlew lib:test --tests "org.test.boxfs.BoxFsIntegrationTest.createAndReadFile"

# Clean build
./gradlew lib:clean lib:build

# Generate JAR
./gradlew lib:jar
```

## Architecture

The file system design (detailed in DESIGN.md) follows these key principles:

- **Metadata in RAM**: Directory structure and file index are loaded into memory at initialization
- **Extent-Based Storage**: Files stored as lists of extents (start_block, block_count) rather than individual blocks
- **Unified Space Management**: Single "Space Manager" handles allocation for both user data and system metadata

### On-Disk Layout

- **Superblock (Sector 0)**: Fixed-location bootstrap containing block size, metadata extents array, and total block count
- **Metadata File**: Serialized index containing Free Space Map, Inode Table (ID, type, size, extent list), and Directory Entries

### Thread Safety

Uses `ReentrantReadWriteLock` at the FileSystem level (many readers, one writer).

## Tech Stack

- Java 25
- Gradle 9.3 with Kotlin DSL
- JUnit Jupiter 5.12.1 for testing
- Dependencies: no dependencies, except for test dependencies

## Project Intent & Goals

- **Java NIO Integration**: The primary goal is to implement a custom `java.nio.file.spi.FileSystemProvider`. The library must allow users to interact with the container using standard Java APIs like `java.nio.file.Files` and `java.nio.file.Path`.
- **NIO Interface Support**: Must implement `FileSystem`, `Path`, `FileSystemProvider`, and `SeekableByteChannel` (for the internal file data).
- **Test Exercise Focus**: This is a coding challenge. Prioritize clean, readable, and idiomatic Java code over exhaustive production-level features (like encryption or compression).

## Java Code Style

This project uses modern Java 25 features. Follow these conventions:

### Type Inference
- **Always use `var`** for local variables where the type is clear from context
- Use `var` for loop variables, try-with-resources, and method return assignments
```java
// Good
var files = new ArrayList<Path>();
var channel = Files.newByteChannel(path, options);
for (var entry : map.entrySet()) { }

// Avoid
ArrayList<Path> files = new ArrayList<Path>();
SeekableByteChannel channel = Files.newByteChannel(path, options);
```

### Records
- Use records for immutable data carriers (DTOs, value objects)
- Records are ideal for simple classes that just hold data
```java
public record Extent(long startBlock, int blockCount) { }
public record DirectoryEntry(long parentId, String name, long childId) { }
```

### Pattern Matching
- Use pattern matching for `instanceof` checks
```java
// Good
if (path instanceof BoxPath boxPath) {
    return boxPath.getFileSystem();
}

// Avoid
if (path instanceof BoxPath) {
    BoxPath boxPath = (BoxPath) path;
    return boxPath.getFileSystem();
}
```

### Switch Expressions
- Use switch expressions with arrow syntax for concise multi-branch logic
```java
var result = switch (type) {
    case FILE -> handleFile();
    case DIRECTORY -> handleDirectory();
    default -> throw new IllegalArgumentException("Unknown type");
};
```

### Text Blocks
- Use text blocks for multi-line strings (JSON, SQL, etc.)

### Other Conventions
- Prefer `List.of()`, `Set.of()`, `Map.of()` for immutable collections
- Use `Optional` for nullable returns, avoid null where possible
- Use `Stream` API for collection transformations
- Prefer method references over lambdas when clearer: `list.forEach(System.out::println)`

### Comments
- **Minimize comments** - code should be self-documenting
- **No section dividers** - avoid `// ====== Section Name ======` style comments
- **No obvious comments** - don't comment what the code clearly does
```java
// Avoid these:
// Create new file
var file = createFile(path);
// Open existing container
var container = openContainer(path);
// Pad with zeros
Arrays.fill(buffer, (byte) 0);
```
- **Keep Javadoc** for public/package API methods that need documentation
- **Complex logic only** - add comments only when the logic is non-obvious or requires explanation
- **Test comments OK** - step-by-step comments in tests are acceptable to document test workflows

## Core Assumptions (Scope Limits)

- **Dynamic Metadata Reservation**: Max metadata extents calculated dynamically based on block size (uses available superblock space).
- **No Path Case-Sensitivity Logic**: Default to standard Java behavior (case-sensitive) to avoid extra complexity.
- **No Symlinks/Hardlinks (Initially)**: While the architecture supports them via Inodes, do not implement them unless the core functionality is finished.
- **In-Memory Tree Consistency**: All changes are made in RAM. Persistence only happens during `fsync` or `close`. We assume no concurrent external modification of the container file.
- **Single Container**: One physical file on the host OS = one entire BoxFS instance.
