# Durability and recovery contract

This contract describes the current embedded engine. It does not claim that every filesystem or storage device provides the same persistence behavior.

## Mutation ordering

A mutation that changes data follows this order under the exclusive database lock:

1. Append the complete encoded record.
2. Synchronize the data file with `File.Sync`.
3. Publish the index change.
4. Return success.

`Delete` of a missing key is a no-op. Input validation happens before storage I/O. Opening an existing database validates all records before synchronizing the file and its directory. Initial creation writes and validates the file header before those synchronization calls. The database directory must already exist; callers own creating it and, when required by their failure model, making its own parent-directory entry durable.

There is no asynchronous acknowledgment mode. Successful `Write` alone is insufficient: it can leave data in the operating system's cache. `File.Sync` requests persistence of the file's contents; directory synchronization addresses the directory entry. The distinction is documented by [Go](https://pkg.go.dev/os#File.Sync) and the [Linux `fsync` manual](https://man7.org/linux/man-pages/man2/fsync.2.html).

## Failure models

| Failure | What can be established | Current validation |
|---|---|---|
| Process exits or is killed; OS remains running | Reopening can recover complete valid records left in the file, including synchronized successful mutations. Kernel caches remain alive. | Reopen tests and subprocess termination tests. |
| OS crash or reboot | Persistence depends on completed file/directory synchronization and the local filesystem and device honoring it. In-flight appends can leave incomplete or corrupt tails. | Synchronization ordering and failures are tested. No OS-crash or VM crash campaign has been performed. |
| Power loss | Also depends on controller/device caches, power-loss behavior, filesystem implementation, and truthful flush completion. | No physical power-cut tests; no unconditional power-loss claim. |

Linux local filesystems are the durability target. macOS is supported for development and testing, but successful `File.Sync` is not presented as proof of equivalent hardware-flush guarantees. Network filesystems and unsupported operating systems are outside the supported contract.

## Uncertain outcomes and faulted handles

If an append is short or returns an error, or file synchronization fails, DiskKV does not update the index and faults the entire handle. Detected read corruption, index/record mismatches, and underlying read failures also fault the handle. Subsequent valid data operations return `ErrFaulted`; close and reopen before attempting further access. Input validation errors retain their own error even on a faulted handle. `Close` releases resources even after a failure.

An error does not roll back bytes already written. A complete record whose `Sync` failed may still be present and replayed on the next open. Likewise, a process can crash after synchronization but before returning success. The engine provides atomic publication of individual successful operations within a running process, not exactly-once delivery or a guarantee that failed calls had no effect.

The faulted state prevents later writes from being appended behind an uncertain tail and prevents continued use of an index that may differ from the recoverable log.

## Strict startup and explicit salvage

`Open` validates the complete log and refuses incomplete headers, incomplete records, bad checksums, invalid metadata, and unsupported versions. It does not silently truncate or skip existing bytes. A zero-byte or partially initialized `data.log` is rejected rather than treated as a new database.

`RepairTail(dir)` is a separate, explicit operation that acquires the same exclusive lock. It never creates a missing data file. It scans the valid prefix and can truncate a structurally incomplete final record, then synchronizes the truncated file. It refuses an incomplete file header, checksum mismatches, invalid lengths, unsupported versions, and arbitrary I/O errors.

This is a salvage policy, not proof that the discarded bytes were unacknowledged. Damage to a file can resemble an interrupted append. Preserve a copy before choosing repair, and inspect its error and returned byte count. If truncation succeeds but the following `Sync` fails, the function returns the number of bytes discarded together with an error: the in-process file size changed, but durable repair is uncertain. A complete valid log returns zero without truncation.

## Ownership and coordination

One database handle owns the data file and an exclusive advisory lock on a separate, persistent `LOCK` file. Keeping a stable lock inode matters when the data file is eventually replaced during compaction. The lock is released when the handle closes or the process terminates. Never unlink `LOCK` while any process may hold it.

Other programs must honor this ownership. Advisory locking cannot prevent an unrelated process from writing directly to `data.log`. Checksums are corruption detectors, not a substitute for coordination or storage redundancy.

## What tests do and do not prove

Truncation tests cover boundaries in the encoded log; corruption tests exercise metadata and payload validation. Fault injection checks that an index change is not published before a successful append and synchronization, and that errors stop subsequent operations. Concurrent workloads run with Go's race detector. Subprocess tests exercise lock release and recovery after process termination.

These tests establish behavior under their injected conditions. They do not simulate every filesystem implementation, controller cache, disk fault, or power-loss sequence. The [race detector](https://go.dev/doc/articles/race_detector) only reports races on paths exercised at runtime.
