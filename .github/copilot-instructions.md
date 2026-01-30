# LLM Agent Instructions

## Project Context

This is BoxFS, a Single-Container File System (SCFS) implementation in Java. Read DESIGN.md for architecture details.

## Code Style

- Use 2-space indentation for Java files
- Follow standard Java naming conventions (camelCase for methods/variables, PascalCase for classes)
- Use `var` for local variables where the type is clear from context
- Keep methods focused and under 30 lines when possible

## Architecture Guidelines

- All metadata operations must be thread-safe using the existing `ReentrantReadWriteLock` pattern
- File content is never fully buffered in RAM; use streaming via `ByteBuffer`
- Extent allocation should use the Space Manager; do not allocate blocks directly
- Path resolution happens in-memory; avoid disk I/O for lookups

## Testing

- Write JUnit Jupiter tests for all new functionality
- Test classes go in `lib/src/test/java/` mirroring the main source structure
- Use descriptive test method names that explain the scenario

## Key Constraints

- Consistency is guaranteed only on graceful `close()` or `sync()`
- File names are limited to 255 bytes
- The container does not shrink automatically; deleted space is recycled via the Free List
