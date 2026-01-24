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
./gradlew lib:test --tests "org.example.LibraryTest"

# Run a single test method
./gradlew lib:test --tests "org.example.LibraryTest.someLibraryMethodReturnsTrue"

# Clean build
./gradlew lib:clean lib:build

# Generate JAR
./gradlew lib:jar
```

## Architecture

The file system design (detailed in DESIGN.md) follows these key principles:

- **Metadata in RAM**: Directory structure and file index are loaded into memory at initialization for O(1) path resolution
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
