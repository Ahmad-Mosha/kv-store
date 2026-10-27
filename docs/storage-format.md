# Storage format, version 1

`data.log` contains a 16-byte file header followed by zero or more records. All integers use unsigned big-endian encoding. Checksums use CRC32C (Castagnoli). The format supports keys containing arbitrary bytes and empty values.

## File header

| Offset | Bytes | Field |
|---|---:|---|
| 0 | 8 | Magic: `44 49 53 4b 4b 56 00 00` (`DISKKV` plus two zero bytes) |
| 8 | 4 | Format version: `1` |
| 12 | 4 | CRC32C of bytes 0–11 |

A valid header is required even for an empty database. A missing or incomplete header is rejected; tail repair cannot reconstruct it. Unknown versions with otherwise valid headers return `ErrUnsupportedVersion`.

## Record header

Each record starts with a 24-byte header:

| Offset | Bytes | Field |
|---|---:|---|
| 0 | 4 | Magic: `44 4b 56 52` (`DKVR`) |
| 4 | 1 | Kind: `1` = set, `2` = delete |
| 5 | 3 | Reserved; must be zero |
| 8 | 4 | Key length |
| 12 | 4 | Value length |
| 16 | 4 | CRC32C of header bytes 0–15 |
| 20 | 4 | CRC32C of the key and value bytes, concatenated |
| 24 | key length | Key bytes |
| 24 + key length | value length | Value bytes |

Key length must be 1–4,096 bytes. Value length must be 0–1,048,576 bytes. A delete record must have a zero value length. Separate metadata and payload checksums allow the decoder to reject a damaged length field before allocating a payload buffer. CRC32C detects accidental corruption; it is not authentication and cannot guarantee detection of every possible modification.

The decoder validates the header, operation kind, reserved bytes, and lengths before allocation. It compares the record span against the known remaining file length without overflowing offset arithmetic. Payload checksums are verified before a record reaches recovery or a caller.

## Replay and indexing

Replay starts at offset 16. A set record replaces the key's index entry with its record offset and encoded size. A delete removes the index entry. Thus the latest valid record for a key defines its current state. DiskKV never scans ahead to guess a new record boundary after corruption.

The in-memory index stores keys and record locations, not values. `Get` uses a positional read and validates the record checksum, kind, size, and key against the index entry. These checks also detect corruption introduced after startup when the affected key is read. `Exists` only checks the index.

## Incomplete and corrupt records

- A nonempty incomplete final header or payload returns `ErrTruncated`.
- Bad magic, checksums, flags, kinds, or lengths return `ErrCorrupt`.
- Other I/O failures retain their underlying error and are not treated as a repair request.
- `Open` fails without truncating the existing log.
- Explicit `RepairTail` scans the same format and may discard only an incomplete final record following a valid file header and valid record prefix.

See [durability and recovery](durability.md) for the limits of this repair policy.
