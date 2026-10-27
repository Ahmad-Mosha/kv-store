# Manual compaction design

**Status: planned for Phase 2; no compaction API is implemented in Phase 1.** This document specifies the replacement protocol and acceptance tests before that code is introduced.

Compaction removes obsolete versions and tombstones by producing one replacement log containing only current live values. The first implementation will hold the database's exclusive lock throughout. This pauses reads and writes but keeps the state transition small enough to reason about and test.

## Replacement protocol

1. Acquire the exclusive database lock and reject a closed or faulted handle.
2. Create a uniquely named replacement file in the same directory as `data.log`.
3. Validate each live source record, including its key, kind, length, and checksums. Write the file header and live records while building a replacement index. Do not turn corrupt source bytes into newly checksummed values.
4. Prepare the replacement file handle and index completely, then synchronize the replacement file.
5. Rename the replacement over `data.log` without first unlinking `data.log`.
6. Synchronize the containing directory.
7. Switch the in-memory file handle, index, and append offset together. Only then release the old handle and resume operations.

The persistent directory `LOCK` is separate from both data-file inodes and stays held throughout. There is no background compaction, segment manifest, concurrent write replay, or automatic promotion of leftover temporary files in this design.

## Failure and recovery matrix

| Stage | Required behavior in the running process | Recovery after process termination |
|---|---|---|
| Temporary-file creation or writing fails | Close/remove the incomplete replacement where possible. Keep the original handle and index. Return the error. A source read-integrity error faults the engine. | `data.log` remains authoritative. Ignore replacement artifacts; never append to or promote them. |
| Replacement `Sync` fails | Do not rename. Discard the replacement where possible and retain the original database. | Recover the original `data.log`; a complete temporary file is not evidence of committed replacement. |
| Replacement is synced, before rename | Original database remains active. | Recover `data.log`; the temporary file is unused. |
| Rename returns an error | Do not publish the replacement index. Treat any uncertain namespace outcome conservatively; establish the target/handle relationship before allowing more writes, otherwise fault the engine. | Validate the authoritative pathname on reopen. Do not guess from a temporary filename. |
| Rename succeeds, directory `Sync` fails | Fault the engine and return an error. Do not resume writes through the old handle: it may refer to an unlinked inode. Preserve error context and close resources safely. | A process-only crash sees the renamed target. After an OS crash or power loss, persistence of the replacement namespace is not established by this failed synchronization. |
| Directory `Sync` succeeds, before handle/index switch | Keep the exclusive lock. Publish only the prepared replacement state; an unexpected failure must fault the handle. | Rebuild from the replacement `data.log`; the in-memory index is disposable. |
| State switch succeeds, old-handle cleanup fails | Do not switch back to the old inode. Report cleanup failure without reverting the published state; the implementation must keep resource ownership explicit. | Rebuild from the replacement `data.log`. |
| Compaction completes | New appends use the replacement handle and offset. | Replay the replacement log normally. |

The pathname chosen for recovery must be validated. A missing or invalid authoritative file in the presence of replacement artifacts must produce an error rather than silently initializing an empty database. Phase 2 must add this startup check before enabling replacement; Phase 1's ordinary create-if-absent path is not a compaction recovery protocol. Temporary files are never automatically promoted.

The OS-crash and power-loss guarantees remain conditional on the filesystem's atomic replacement and synchronization contracts. Go explicitly notes that rename is not atomic on every platform; see [`os.Rename`](https://pkg.go.dev/os#Rename). This design targets tested Linux/macOS local filesystem behavior, not universal crash guarantees.

## Required fault-injection and recovery tests

- Replacement creation, short write, write error, and file-sync failure leave the original logical state recoverable.
- A corrupt live source record is rejected and never rewritten with a fresh valid checksum.
- Failures immediately before rename, during replacement, and after successful rename do not resume appends through the old handle.
- Directory-sync failure after rename faults the database and prevents reads and mutations until close/reopen.
- Termination after each replacement stage recovers the same logical key/value state through the surviving authoritative log.
- Recovery ignores leftover temporary files and rejects ambiguous/missing authoritative-file cases instead of creating empty state.
- The prepared index points into the new file, append resumes at its end, deleted keys stay absent, and a later restart preserves these properties.
- Failed cleanup never replaces the new index with stale offsets or releases the directory lock early.
- Compaction of an empty database, an update-heavy log, and a deletion-heavy log preserves exact logical state.

Process termination tests do not establish OS-crash or power-loss behavior. Until a separate crash campaign exists, documentation and benchmark results must state that boundary explicitly.
