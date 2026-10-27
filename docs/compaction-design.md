# Manual compaction and recovery

`DB.Compact()` replaces the append-only log with a log containing only current live records. It holds the database's exclusive lock throughout, pausing reads and writes. This keeps the replacement state transition explicit; background compaction and concurrent write replay are outside the MVP.

## Replacement protocol

1. Acquire the exclusive database lock and reject a closed or faulted handle.
2. Create a sibling `.compact-*` temporary file in the database directory.
3. Validate each live source record against its indexed key, kind, size, and checksums. Write the version 1 file header and the live records, building a replacement index and append offset. Corrupt source values are never rewritten with fresh checksums.
4. Synchronize the replacement file. Its writable handle and new index are ready before namespace replacement.
5. Rename the replacement over `data.log` without unlinking the original first.
6. Synchronize the containing directory.
7. Publish the replacement handle, index, and append offset together, then close the old handle before releasing the exclusive lock.

The stable `LOCK` inode remains locked throughout. Source and value reads use `ReadAt`, so they do not move the replacement handle's writable cursor. Once installed, that cursor remains at the end of the encoded log for subsequent appends.

The temporary file must coexist with the original before replacement. Compaction therefore needs extra disk space for the live log and temporarily retains both indexes in memory. It does not promise a particular record order or byte-for-byte reproducibility; the logical key/value state is preserved.

## Failure and recovery matrix

| Stage | Running-process behavior | Recovery after process termination |
|---|---|---|
| Temporary-file creation fails | Return the error; retain the original handle and index. | Recover the original `data.log`. |
| Source read or integrity check fails | Fault the database, reject subsequent valid operations, and clean up the temporary file where possible. | Strictly validate `data.log`; corrupt source data is not repaired by compaction. |
| Replacement header/record write fails, including a short write | Do not rename. Close/remove the temporary file where possible and retain the usable original database. | Recover `data.log`; any leftover temporary file is ignored. |
| Replacement file `Sync` fails | Do not rename or publish the new index. Retain the usable original database. | Recover `data.log`; a complete temporary file does not prove that replacement committed. |
| Replacement is synced, before rename | Original state remains active under the exclusive lock. | Recover the original `data.log`. |
| Rename returns an error | Fault the handle without publishing the new index. This applies even when replacement actually took effect before an error was reported. Never resume writes through the old handle. | Validate the surviving authoritative `data.log`; do not infer its identity from the returned error. |
| Rename succeeds, directory `Sync` fails | Fault the handle. Do not append through the old descriptor, which may refer to an unlinked file. | A process-only crash sees the renamed target. After OS failure or power loss, the failed directory synchronization does not establish durable replacement. |
| Directory `Sync` succeeds, before publication | Keep operations excluded until the prepared replacement state is published. | Rebuild from the replacement `data.log`; the in-memory index is disposable. |
| Publication succeeds, old-file `Close` fails | Return the cleanup error, retain the active replacement handle/index, and do not roll back or fault solely for that cleanup error. | Recover the replacement `data.log`. |
| Compaction completes | Resume operations using the replacement handle and offset. | Replay the replacement log normally. |

Cleanup closes the temporary handle and removes only its original temporary pathname when applicable. It never removes `data.log` after replacement. Cleanup errors are joined with the original error. Consequently, an error can leave an orphan temporary file, and a cleanup error after publication can coexist with a successfully installed replacement.

## Startup authority and initialization evidence

`data.log` is authoritative when it exists. Startup validates it and neither promotes nor automatically deletes `.compact-*` files. This prevents an incomplete or obsolete replacement from being mistaken for committed state.

After validating and synchronizing an opened log and its directory, `Open` writes and synchronizes a one-byte initialization marker in the persistent `LOCK` file. This marker is coordination evidence, not a manifest or another database representation. Existing valid logs with an empty lock file are marked by the same open path without changing the log format.

If `data.log` is missing and either `LOCK` is nonempty or `.compact-*` artifacts exist, opening returns `ErrCorrupt`. It does not silently create an empty database. A missing log can be created only with an empty lock and no replacement artifacts. Preserve `data.log` and `LOCK` together when copying a closed database directory; deleting lock evidence is not a recovery procedure.

## Validation boundaries

Fault-injection tests cover temporary-file creation, writes, synchronization, rename errors before and after replacement takes effect, directory synchronization, and cleanup. Logical-state tests verify that deleted keys stay absent, the replacement index addresses the new records, and writes after compaction survive reopen. Startup-guard tests cover missing data files, existing initialization evidence, orphan replacements, and earlier unmarked valid logs.

Subprocess termination tests stop execution around replacement stages and reopen the database. They establish process-crash behavior with the OS and its caches still alive. They do not simulate OS crashes or physical power loss.

Replacement durability remains conditional on the filesystem's atomic replacement and synchronization contracts. Go notes that rename is not atomic on every platform; see [`os.Rename`](https://pkg.go.dev/os#Rename). Supported Linux/macOS local filesystem behavior and the [durability contract](durability.md) define the project's scope; there is no universal power-loss claim.
