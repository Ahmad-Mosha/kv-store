# TCP protocol

DiskKV exposes `SET`, `GET`, `DELETE`, and `EXISTS` over TCP. Requests and responses are JSON objects, each terminated by a newline (`\n`). JSON strings encode binary keys and values using standard base64. Connections may carry multiple requests; the server executes requests in order on each connection and emits one response for each accepted frame. There are no request identifiers or automatic retries.

The default listener is `127.0.0.1:7379`. The protocol provides no authentication, TLS, or network compaction command. Use a trusted local interface or secure the connection separately.

## Requests

| Field | Type | Contract |
|---|---|---|
| `op` | string | Exactly `SET`, `GET`, `DELETE`, or `EXISTS`. |
| `key` | base64 string | 1–4,096 decoded bytes. |
| `value` | base64 string | SET value, 0–1,048,576 decoded bytes. An omitted or `null` value is empty; other operations may not include a nonempty value. |

For example, these four frames use the key `city` and value `Cairo`:

```json
{"op":"SET","key":"Y2l0eQ==","value":"Q2Fpcm8="}
{"op":"GET","key":"Y2l0eQ=="}
{"op":"EXISTS","key":"Y2l0eQ=="}
{"op":"DELETE","key":"Y2l0eQ=="}
```

Each displayed line must end with a newline on the wire. TCP reads and writes do not correspond to frame boundaries: a frame may arrive in fragments, and multiple frames may arrive in one read. CRLF is accepted as JSON trailing whitespace plus newline. The carriage return counts toward the frame-size limit.

## Responses

| Result | Response |
|---|---|
| SET or DELETE succeeds | `{"ok":true}` |
| GET succeeds | `{"ok":true,"value":"Q2Fpcm8="}` |
| GET returns an empty value | `{"ok":true,"value":""}` |
| EXISTS succeeds | `{"ok":true,"exists":true}` or `{"ok":true,"exists":false}` |
| Operation fails | `{"ok":false,"error":{"code":"NOT_FOUND","message":"key not found"}}` |

Deleting a missing key succeeds. GET on a missing key returns `NOT_FOUND`. Error messages are sanitized; responses do not disclose storage paths or raw underlying errors.

| Error code | Meaning | Connection behavior |
|---|---|---|
| `BAD_REQUEST` | Unsupported command, invalid key/value size, or malformed frame. | Complete JSON requests with invalid command semantics remain usable; malformed frames close after an attempted error response. |
| `FRAME_TOO_LARGE` | JSON bytes exceed the frame limit. | Closes after an attempted error response; excess bytes are not drained. |
| `NOT_FOUND` | GET key is absent. | Remains usable. |
| `RATE_LIMITED` | The peer IP has exhausted its token budget. | Remains usable. |
| `UNAVAILABLE` | Storage is closed/faulted, or the limiter failed. A storage failure can leave a mutation's outcome uncertain. | Remains usable, but subsequent requests may also fail. |
| `INTERNAL_ERROR` | Another storage error occurred. | Remains usable; mutation outcome may be uncertain. |

Rate-limit failures include a positive delay rounded up to milliseconds:

```json
{"ok":false,"error":{"code":"RATE_LIMITED","message":"request limit exceeded","retry_after_ms":20}}
```

This delay is advisory; other connections sharing the IP may consume tokens before a retry. No mutation executes when request admission fails.

## Parsing and resource limits

The frame limit is 2,097,152 bytes of JSON excluding the final newline, including any JSON whitespace. Key and value limits apply after base64 decoding. Oversized input is detected while framing without reading the entire oversized message into memory. The reader bounds its accumulated frame allocation at 2 MiB and has a separate 4 KiB read buffer; decoding and execution require additional memory.

Only one JSON object is allowed per frame. Duplicate fields, unknown fields, trailing JSON, wrong scalar types, numeric byte arrays, and invalid base64 are rejected. A null root is invalid. A complete object without the terminating newline is incomplete input, including when followed by EOF. Clean EOF between frames ends the connection silently. Network I/O failures and read deadlines also close the connection without a guaranteed error frame.

Each accepted connection has one handler goroutine and processes one command at a time. The connection limit is independent of rate limiting. A complete frame is decoded, then charged against the IP bucket, then checked for supported operation and key/value sizes. Malformed frames close during parsing; they are not admitted to storage. A rate-limited frame has already been consumed, so the next frame can be read normally.

The budget key is the actual peer IP from the TCP socket without its source port. IPv4-mapped IPv6 addresses are normalized to IPv4. Reconnecting does not reset the budget. There is no proxy header or client-provided identity field.

The reused in-memory limiter periodically removes idle buckets, but does not cap distinct IP entries. A sustained stream of new IPs can grow its map despite the connection cap. Budget state is neither persistent nor shared across server processes.

## Server configuration

```sh
go run ./cmd/diskkv -data ./data -addr 127.0.0.1:7379
```

| Flag | Default | Validation |
|---|---|---|
| `-addr` | `127.0.0.1:7379` | TCP host and numeric port from 0 to 65,535. |
| `-data` | `./data` | Nonempty directory path; created if needed. |
| `-max-connections` | `128` | Integer from 1 to 4,096. |
| `-read-timeout` | `30s` | Positive duration, at most 24h. |
| `-write-timeout` | `5s` | Positive duration, at most 24h. |
| `-shutdown-timeout` | `5s` | Positive duration, at most 24h. |
| `-burst` | `100` | Integer capacity from 1 to 1,000,000 tokens. |
| `-rate` | `50` | Finite positive tokens per second; `burst/rate` must be at most 24h. |

Validation finishes before creating the directory, opening storage, starting the bucket, or listening. Configured durations are limits for specific stages, not storage-operation deadlines. The read deadline covers an entire frame and does not reset for each fragment. The write deadline covers a response. There is no request deadline that cancels a file `Sync`.

Capacity excess closes the newly accepted socket immediately without starting another handler or sending an error response. These limits do not cap the database's disk usage, live-key count, total index memory, or the cumulative memory used by all decoded frames.

## CLI

```sh
go run ./cmd/diskkv-cli set city Cairo
go run ./cmd/diskkv-cli get city
go run ./cmd/diskkv-cli exists city
go run ./cmd/diskkv-cli delete city
go run ./cmd/diskkv-cli -base64 set YmluYXJ5 AP8K
go run ./cmd/diskkv-cli -base64 get YmluYXJ5
```

Flags must precede the command. `-addr` selects the server using a host and numeric port; `-timeout` sets a positive total timeout, at most 24h, for hostname resolution, dialing, and the request/response exchange (default 5s). `-base64` decodes key/value arguments and encodes GET output. SET and DELETE print `OK`; EXISTS prints `true` or `false`; plain GET writes the stored bytes followed by a newline. Errors go to stderr with a nonzero exit status.

The client makes one attempt. It never retries after connection failure, timeout, rate limiting, or an uncertain response. It reports uncertain outcomes for SET/DELETE transport failures and `UNAVAILABLE` or `INTERNAL_ERROR` responses. A failed synchronization can leave a complete record that reappears during recovery, even when an error response was received. A mutation whose response is lost may already be durable. Retrying a SET can overwrite an intervening writer even when the same bytes are sent.

## Shutdown and durability

SIGINT or SIGTERM stops accepting new connections and closes idle connections, including incomplete frames. Commands admitted before shutdown may finish, including writing their response. Buffered subsequent requests are not admitted after shutdown starts. Normal shutdown waits for handlers before closing the database and stopping the bucket.

If draining exceeds `-shutdown-timeout`, remaining sockets close and the command exits with an error. Active filesystem I/O cannot be canceled by socket closure. The command avoids waiting indefinitely in `DB.Close` in this case and relies on process exit to release file descriptors and the advisory lock. Request outcomes may remain uncertain. The timeout bounds the drain wait; it does not guarantee a bound for every operating-system call or final database cleanup.

A successful SET or effective DELETE response follows the engine's append and file synchronization. This ordering remains subject to the [durability contract](durability.md). Process termination tests do not simulate an OS crash or power loss, and networking adds no stronger persistence guarantee.
