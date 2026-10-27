# Durability and recovery contract

This contract describes the current embedded engine. It does not claim that every filesystem or storage device provides the same persistence behavior.

## Mutation ordering

A mutation that changes data follows this order under the exclusive database lock:

1. Append the complete encoded record.
2. Synchronize the data file with `File.Sync`.
3. Publish the index change.
4. Return success.

`Delete` of a missing key is a no-op. Input validation happens before storage I/O. Opening an existing database validates all records before synchronizing the file and its directory. Initial creation writes and validates the file header before those synchronization calls. After that sequence, `Open` writes and synchronizes initialization evidence in `LOCK` before returning. The database directory must already exist; callers own creating it and, when required by their failure model, making its own parent-directory entry durable.

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

## Compaction ordering and errors

Manual compaction holds the exclusive database lock while it validates live source records and writes a replacement log. The publication order is replacement-file `Sync`, rename over `data.log`, directory `Sync`, then replacement handle/index/offset publication. The old handle is closed only after publication. See the [complete failure matrix](compaction-design.md).

Temporary-file creation, writing, or synchronization failures before rename retain the original handle and index; that database remains usable. Source-read or integrity errors fault the handle. Every rename error faults it because the engine does not assume the namespace remained unchanged after an error. A directory-sync failure after a successful rename also faults it: writing through the old descriptor could otherwise append into an unlinked file. Closing and reopening revalidates the authoritative `data.log`.

Failure closing the old file after publication is different: compaction has already installed the replacement, so the error reports cleanup failure and the new state remains active. It is not rolled back. Temporary-file cleanup errors are joined with the original failure rather than hidden. A compaction error must therefore be interpreted with its stage; it does not always mean the original file is still active.

Process-termination tests around these stages leave the OS and its caches alive. They validate the restart protocol in that failure model, not whether a rename or directory update survives OS failure or power loss. Replacement durability still depends on successful synchronization and the underlying storage contracts.

## Strict startup and explicit salvage

`Open` validates the complete log and refuses incomplete headers, incomplete records, bad checksums, invalid metadata, and unsupported versions. It does not silently truncate or skip existing bytes. A zero-byte or partially initialized `data.log` is rejected rather than treated as a new database.

Successful initialization leaves a one-byte marker in the stable `LOCK` file. If `data.log` is missing and either `LOCK` is nonempty or any `.compact-*` artifacts remain, `Open` returns `ErrCorrupt` instead of creating an empty database. Only a missing log with an empty lock and no compaction artifacts can be initialized. An existing valid log with an empty lock, including one created by the earlier engine, is marked after successful validation and synchronization. The marker is initialization evidence, not a manifest or an alternative source of key/value data.

When `data.log` exists, it is authoritative: startup validates it and ignores leftover compaction files. It does not promote temporary files, delete them automatically, or use them to bypass damage in the authoritative log.

`RepairTail(dir)` is a separate, explicit operation that acquires the same exclusive lock. It never creates a missing data file. It scans the valid prefix and can truncate a structurally incomplete final record, then synchronizes the truncated file. It refuses an incomplete file header, checksum mismatches, invalid lengths, unsupported versions, and arbitrary I/O errors.

This is a salvage policy, not proof that the discarded bytes were unacknowledged. Damage to a file can resemble an interrupted append. Preserve a copy before choosing repair, and inspect its error and returned byte count. If truncation succeeds but the following `Sync` fails, the function returns the number of bytes discarded together with an error: the in-process file size changed, but durable repair is uncertain. A complete valid log returns zero without truncation.

## Ownership and coordination

One database handle owns the data file and an exclusive advisory lock on a separate, persistent `LOCK` file. The lock inode stays stable when compaction replaces the data file. The lock is released when the handle closes or the process terminates. Never unlink `LOCK` while any process may hold it, and preserve its initialization marker between runs.

Close the database before copying or moving its directory, and preserve `data.log` and `LOCK` together. Copying only an arbitrary live log is not a snapshot protocol. Removing initialization evidence can defeat the missing-file guard; it is not a repair procedure.

Other programs must honor this ownership. Advisory locking cannot prevent an unrelated process from writing directly to `data.log`. Checksums are corruption detectors, not a substitute for coordination or storage redundancy.

## What tests do and do not prove

Truncation tests cover boundaries in the encoded log; corruption tests exercise metadata and payload validation. Fault injection checks mutation ordering, compaction replacement stages, startup guards, and cleanup errors. Concurrent workloads run with Go's race detector. Subprocess tests exercise lock release and recovery after process termination, including compaction-stage termination.

These tests establish behavior under their injected conditions. They do not simulate every filesystem implementation, controller cache, disk fault, or power-loss sequence. The [race detector](https://go.dev/doc/articles/race_detector) only reports races on paths exercised at runtime.
