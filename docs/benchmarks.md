# Benchmark methodology and results

These benchmarks measure the current storage and TCP paths without changing their durability policy. They are intended to expose costs and limits within DiskKV. They do not compare DiskKV with other databases.

## Local observations

Measured on 27 October 2026 at code revision [`463f356`](https://github.com/Ahmad-Mosha/kv-store/commit/463f356), using Go 1.26.0, macOS 26.5.2, an Apple M1 Pro with 10 cores and 16 GiB RAM, and an internal SSD with APFS and FileVault enabled. The machine was on AC power. The [complete raw output](benchmarks/2026-10-27-macos-arm64.txt) preserves all 170 samples and the command.

Each cell reports the median of five runs; the range is the observed minimum to maximum. These are averages within each run, not request-latency percentiles. No samples were discarded.

The storage table summarizes `GOMAXPROCS=1`; the raw output also preserves every storage case at `GOMAXPROCS=4`. The concurrency table compares both settings. Variation is substantial in some runs: 10,000-record recovery at `GOMAXPROCS=4` ranged from 18.746 to 52.753 ms.

### Storage at GOMAXPROCS=1

| Workload | Median per operation | Observed range | Allocated B/op, median |
|---|---:|---:|---:|
| Get, 64 B | 0.957 µs | 0.941–1.001 µs | 104 |
| Get, 1 KiB | 1.184 µs | 1.158–1.199 µs | 1,176 |
| Get, 64 KiB | 18.221 µs | 16.845–18.925 µs | 73,752 |
| Set + Sync, 64 B | 3.896 ms | 3.838–3.985 ms | 128 |
| Set + Sync, 1 KiB | 4.310 ms | 4.114–4.412 ms | 1,168 |
| Set + Sync, 64 KiB | 4.730 ms | 4.627–5.139 ms | 73,744 |
| File append, write only | 4.719 µs | 4.663–4.900 µs | 0 |
| File append + Sync | 4.153 ms | 4.098–4.345 ms | 0 |
| Open + Close, 1,000 records | 7.601 ms | 6.947–8.382 ms | 489,923 |
| Open + Close, 10,000 records | 25.676 ms | 25.134–28.382 ms | 3,441,951 |
| Compact, 500 live keys | 12.021 ms | 11.261–19.177 ms | 358,312 |

### Concurrency and TCP

These times divide elapsed wall time by all completed operations. Engine workers follow `GOMAXPROCS`; TCP client counts remain fixed at one or four in both columns. The table shows median aggregate µs/op with the observed range in parentheses.

| Workload, 1 KiB values | GOMAXPROCS=1 | GOMAXPROCS=4 |
|---|---:|---:|
| Engine read-only | 2.466 (1.890–2.562) | 3.155 (2.367–3.718) |
| Engine 95% reads / 5% writes | 223.330 (214.562–249.493) | 226.448 (217.339–237.883) |
| TCP GET, 1 client | 59.839 (58.656–67.238) | 58.196 (57.274–59.148) |
| TCP GET, 4 clients | 40.337 (38.943–45.381) | 24.806 (24.705–24.925) |
| TCP 95% reads / 5% writes, 1 client | 308.088 (281.349–318.547) | 299.933 (296.880–312.911) |
| TCP 95% reads / 5% writes, 4 clients | 304.036 (270.404–317.229) | 241.850 (236.508–253.510) |

### What the measurements show

- Per-mutation synchronization dominates small writes on this machine. The bounded write-only microbenchmark avoids that request and is not an engine durability option. Batching could amortize synchronization, but would require a separate acknowledgment policy and is outside this implementation.
- Additional engine workers did not improve this small warm-cache read workload. More goroutines do not establish scalability; attributing the regression to a particular lock or allocator would require profiling. The mixed workload remains limited by serialized synchronized writes while readers wait for the exclusive lock.
- Four TCP clients at GOMAXPROCS=4 completed about 40.3 thousand GETs/s in aggregate, while one completed about 17.2 thousand/s. These are loopback observations with a raised rate limit, not default server capacity or per-request latency. Mixed writes show a much smaller gain.
- The full TCP GET path allocated about 19,200 B/op versus 1,176 B/op for embedded Get. This exposes the allocation cost of the current protocol and client/server path; it is not a retained-memory comparison.
- Recovery scans historical records even when live-key counts remain similar. Startup also includes synchronization, so these measurements are not parser-only throughput. The compaction fixture reduced logical log length by 95.0%, from 2,980,016 to 148,016 bytes, while pausing other engine operations.

## Reproduce

Run from the repository root on Linux or macOS:

```sh
go test -p 1 ./engine ./internal/server -run '^$' -bench . -benchmem -benchtime=500ms -count=5 -cpu=1,4
```

`-p 1` runs the two packages sequentially so their benchmarks do not compete with each other. `-cpu=1,4` measures both `GOMAXPROCS` settings; it does not pin workers to specific physical cores. Run without the race detector or coverage instrumentation when collecting performance results. Use a quiet machine and record its environment. Each benchmark uses disposable data under Go's temporary directory.

CI runs the workloads once with `-benchtime=1x` to catch broken fixtures or execution paths. It applies no performance threshold. Shared CI runners are not the source of the published measurements.

## Workloads

The engine benchmarks use fixed 16-byte keys. Fixture construction and fixture verification are excluded from the measured operations. Per-operation error and result checks remain in the workload. Get and Set cycle through 1,024 live keys at value sizes of 64 B, 1 KiB, and 64 KiB. Concurrent engine workloads use 1 KiB values and Go's `RunParallel`, with one worker per `GOMAXPROCS`. Mixed workloads select a write every twentieth operation, with rounding at worker boundaries; writes advance through their own key cycle so every live key is eligible.

| Benchmark | Measured work |
|---|---|
| `Get` | Read and checksum a value from a prepopulated, warm-cache database. |
| `Set` | Replace a key, including encoding, append, file `Sync`, and index publication. |
| `Concurrent` | Shared-engine reads or a deterministic mix of 95% reads and 5% synchronized writes. |
| `FileAppend` | Append an already encoded record with or without a `Sync` after each write. This isolates filesystem-call overhead; the unsynchronized case is not a supported engine mode. |
| `Recovery` | Full `Open` and `Close`, including log validation, index reconstruction, initialization evidence, and file/directory synchronization. |
| `Compaction` | Compact the same logical fixture on every iteration, including live-record validation, replacement writes, file synchronization, rename, directory synchronization, and handle/index publication. |
| `TCP` | Persistent loopback clients executing request/response exchanges through JSON/base64 framing, the actual token bucket, and the real engine. |

Reads and recovery reuse recently created files. OS caches are not dropped, so these are warm-cache measurements. They do not measure cold startup, physical disk read latency, or a dataset larger than RAM. Updates retain a fixed live-key set while the append-only log grows during the run.

Recovery fixtures contain 1,000 or 10,000 SET records, 256-byte values, and `min(1024, records)` live keys. The compaction fixture contains 10,000 SET records across 1,000 keys, followed by 500 DELETE records. Its logical file length decreases from 2,980,016 to 148,016 bytes; this is not a measurement of physical SSD blocks reclaimed. A fresh copy is opened and validated before every timed compaction. TCP seeds its fixture through the public API once, then each calibrated sample opens a separate fresh log copy outside the timer.

The TCP benchmark uses 1 KiB values and 1,024 live keys, with exactly one or four persistent clients. Client deadline updates, request/response encoding and decoding, and returned-value checks are timed. Its actual limiter has a capacity of 1,000,000 and refills 1,000,000,000 tokens per second to measure successful requests. It does not measure rejection throughput or the default 50-request-per-second admission policy. Parallel `ns/op` is total wall time divided by completed operations: its reciprocal describes aggregate throughput, not the latency of an individual request. `MB/s` counts decoded value bytes per operation rather than wire bytes. No percentile latency claims are made.

`B/op` and `allocs/op` describe allocations during the timed interval. They are not retained index size or peak process memory. Concurrent TCP measurements include allocations in server goroutines in the same process.

## Interpretation limits

The append comparison writes a pre-encoded 1,064-byte record containing a 1 KiB value and excludes encoding and index work from both cases. At 64 MiB the file is truncated and its cursor reset while timing is stopped, bounding temporary storage. The write-only case can discard unsynchronized dirty pages during this reset. It measures the bounded OS-buffered call path, not sustained disk throughput or eventual flush cost. The comparison exposes the extra cost of calling `File.Sync` after each write in this environment; filesystem writeback, caches, file growth, resets, and run-to-run variation remain involved. It does not establish the cost of a power-loss guarantee. In particular, macOS `File.Sync` is not presented as an equivalent hardware-flush guarantee to every Linux storage stack. See the [durability contract](durability.md).

Compaction measurements include the operation's exclusive-lock pause, while fixture restoration happens outside the timer. Compaction runs without concurrent clients: this is an uncontended operation time, not a worst-case pause under load. Recovery includes startup synchronization and is not a parser-only throughput test. These choices describe user-visible operations rather than isolated implementation loops.

The results are local observations from one machine. There is no CPU affinity, cache eviction, storage fault simulation, or controlled thermal environment. Repeated-run ranges show observed variation, not confidence intervals or guarantees for deployment hardware.
