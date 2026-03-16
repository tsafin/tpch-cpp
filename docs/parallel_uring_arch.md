# Parallel TPC-DS Generation with General io_uring Layer

**Status**: Design / pre-implementation
**Target branch**: `tpcds_cpp_embedded`
**Phase label**: DS-10

---

## Motivation

TPC-DS has 24 tables. The current `tpcds_benchmark` generates them one at a time,
single-threaded. At SF=10 the slowest table (`store_sales`, 28.8 M rows) takes ~144 s
alone; generating all 24 tables sequentially takes ~1 h. The goal is to close that gap
by two orthogonal improvements:

1. **Parallel table generation** — all 24 tables generated simultaneously (fork-after-init).
2. **General async write layer** — a format-agnostic `io_uring`-backed Arrow
   `OutputStream` shared by all writers, activated automatically when `--parallel` is used.

---

## Lessons from Past Experiments

### TPC-H Phase 12.3 — per-child re-init (broken)

Each forked child called `dbgen_init_global()` independently.
Problem: re-initialization reset the table-partitioned seed arrays, producing wrong row
counts and duplicate data.
Fix (Phase 12.6): init once in the parent, fork children that call `set_skip_init(true)`.

### TPC-H Phase 12.6 — fork-after-init (working)

```
parent: dbgen_init_global()
  └── fork × 8
        child: set_skip_init(true) → generate one table → exit
parent: waitpid × 8
```

Seeds are partitioned by table in `Seed[]`; children never touch sibling seeds.
Result: ~8× throughput for all-tables generation.

### Lance io_uring (Phase 3.4 / io_uring v3, working)

Implemented in `third_party/lance-ffi/src/io_uring_store.rs`.
Key benchmark (SF=10 lineitem, 60 M rows):

| | Wall time | Avg bandwidth | Stall time |
|---|---|---|---|
| Baseline | 183.9 s | 43.9 MB/s | 144.6 s (78%) |
| io_uring v3 | **85.7 s** | **188.5 MB/s** | 57.5 s (67%) |
| **Speedup** | **2.15×** | **4.3×** | **2.5× less stalled** |

Discoveries that must be generalised (see next section):

| # | Discovery | Rationale |
|---|-----------|-----------|
| 1 | `IORING_SETUP_ATTACH_WQ` shared kernel worker pool | Reduces scheduler pressure; prevents host stalls on WSL2 |
| 2 | Persistent ring per file | Eliminates ~800 `io_uring_setup()` syscalls for a 4 GB file |
| 3 | sysfs queue-depth calibration | Reads `/sys/block/*/queue/nr_requests`, clamps to [8, 128] |
| 4 | No `SQPOLL` | WSL2: SQPOLL creates busy-polling kernel thread → Windows scheduler freeze |
| 5 | No `O_DIRECT` | WSL2/VirtIO: each O_DIRECT write waits for disk ACK, kills pipelining |
| 6 | Worker thread owns ring for file lifetime | No per-write ring setup; amortises setup cost |
| 7 | Async MPSC channel for write dispatch | Decouples generation from disk latency |
| 8 | Atomic offset pre-claiming | Lock-free: `fetch_add(len)` before async write, enables out-of-order completion |

### Parquet `AsyncIOContext` (C++ side, partial)

`include/tpch/async_io.hpp` has a complete io_uring API
(`queue_write`, `register_buffers`, `submit_queued`, …) gated by
`#ifdef TPCH_ENABLE_ASYNC_IO`.
The Parquet writer has `set_async_context()` but it is wired only to the old
batch-accumulation path — **not** to the streaming (`--zero-copy`) path which calls
`FileWriter::WriteRecordBatch()` directly on an `arrow::io::FileOutputStream`.

The async context API is a good foundation but the integration layer is missing.

---

## Why Not a Pre-forked Job Server?

Make's `-j N` job server pre-forks workers because jobs are discovered dynamically
as the build graph expands. Workers stay alive across many short-lived tasks to amortise
the fork cost and avoid repeated initialisation.

For TPC-DS: the work set is **entirely static** — exactly 24 tables, all known before
any fork. A pre-forked pool would require IPC (pipe/socket) to assign table IDs to
workers, a protocol for "done, give me more", and careful handling of the case where a
worker inherits the wrong dsdgen stream state. That is 100+ lines of IPC plumbing
solving a problem that does not exist here.

The only desirable property borrowed from the job-server pattern is the **N-slot rolling
window** — don't start table N+1 until one of the N active slots is free. This is
naturally expressed with io_uring as described below.

---

## Architecture

### Guiding principles

> 1. io_uring is activated by `--parallel`, not by `--<format>-io-uring`.
>    Format writers are oblivious to the I/O backend.
> 2. The parent's anchor ring is the **scheduler** (process lifecycle) AND the
>    **I/O pool anchor** (ATTACH_WQ). One mechanism, two roles.

### io_uring as the concurrency semaphore

Rather than a POSIX semaphore, pipe-token pool, or `WNOHANG` poll loop, the parent
uses `pidfd` + `IORING_OP_POLL_ADD` to implement the N-slot rolling window directly
in the ring:

- `pidfd_open(child_pid, 0)` (Linux ≥ 5.3) returns an fd that becomes readable when
  the child exits.
- Submit `IORING_OP_POLL_ADD(pidfd, POLLIN)` to the anchor ring — one SQE per live
  child.
- **The number of in-flight POLL_ADD entries IS the semaphore count**: never more than
  N SQEs live in the ring, so never more than N tables running.
- When a CQE fires: reap the child (`waitid(P_PIDFD, ...)`), close the pidfd,
  decrement active count, fork the next table if any remain.

No separate semaphore object. No busy-waiting. `io_uring_enter(min_complete=1)` blocks
until a slot opens.

```
// Scheduler loop in parent (pseudocode)

ring = io_uring_setup(QD, flags=0)          // anchor ring
init_dsdgen()                               // distributions loaded once

// Fill initial N slots
for i in 0 .. min(N, tables.size()):
    pid        = fork_child(tables[i])
    pidfds[i]  = pidfd_open(pid)
    ring.submit(POLL_ADD, pidfds[i], POLLIN, user_data=i)
    active++

// Rolling scheduler loop
while active > 0:
    cqe = ring.wait(min_complete=1)         // block — no spin
    for each cqe in ring.peek_batch():
        idx  = cqe.user_data
        waitid(P_PIDFD, pidfds[idx], ...)   // reap zombie
        close(pidfds[idx])
        active--
        if next_table < tables.size():
            pid          = fork_child(tables[next_table])
            pidfds[idx]  = pidfd_open(pid)
            ring.submit(POLL_ADD, pidfds[idx], POLLIN, user_data=idx)
            next_table++;  active++

unlink(tmp_dist_path)                       // parent owns cleanup
```

The same `ring` fd is inherited by all children as the `ATTACH_WQ` anchor — children
create their I/O rings with `IORING_SETUP_ATTACH_WQ, wq_fd=ring_fd`, sharing the
kernel async-worker pool with the parent scheduler.

### Full picture

```
parent
├── init_dsdgen()                      ← one-time; all streams ready
├── ring = io_uring_setup(QD, 0)       ← anchor: scheduler + ATTACH_WQ source
│
├── [scheduler loop, N slots active]
│     POLL_ADD(pidfd[i]) → CQE when child[i] exits
│     on CQE: reap, fork next, re-submit POLL_ADD
│
└── fork() × 24 (rolling, ≤ N at a time)
      │
      ├── child [store_sales]
      │     child_ring = io_uring_setup(QD, ATTACH_WQ, wq_fd=parent_ring_fd)
      │     stream  = IoUringOutputStream(path, child_ring)
      │     writer  = create_writer(format, stream)   ← injected, format-agnostic
      │     generate → write_batch() → stream.Write() → SQE on child_ring
      │     close() → drain CQEs → join worker thread → exit(0)
      │
      ├── child [inventory]           (same pattern)
      └── ...
          all children share one kernel async-worker pool via ATTACH_WQ
```

### Kernel version requirements

| Feature | Minimum kernel | Available on 6.6 WSL2 |
|---------|---------------|----------------------|
| `io_uring` | 5.1 | ✅ |
| `pidfd_open` | 5.3 | ✅ |
| `IORING_OP_POLL_ADD` on pidfd | 5.3 | ✅ |
| `IORING_SETUP_ATTACH_WQ` | 5.6 | ✅ |
| `IORING_OP_WAITID` (alternative) | 6.7 | ❌ (not needed; pidfd approach used) |

### `IoUringPool` — dual-role anchor ring manager

The anchor ring serves two independent roles after `init()`:

1. **Scheduler ring** (parent only): submits `POLL_ADD(pidfd)` SQEs and waits for
   CQEs to detect child exits. Ring depth = `--parallel-tables N` = semaphore count.
2. **ATTACH_WQ source** (inherited by children): each child calls `create_child_ring()`
   which creates its I/O ring with `IORING_SETUP_ATTACH_WQ` pointing at the anchor fd.
   One kernel async-worker pool serves all I/O across all children.

```cpp
// include/tpch/io_uring_pool.hpp
class IoUringPool {
public:
    // Called once in parent before any fork.
    // Creates anchor ring with QD calibrated from sysfs.
    // output_dir used for device detection.
    static void init(const std::string& output_dir, uint32_t max_parallel);

    // Parent scheduler: submit POLL_ADD for a child pidfd.
    // user_data is returned verbatim in the CQE.
    static void watch_child(int pidfd, uint64_t user_data);

    // Parent scheduler: block until at least one child exits.
    // Returns completed user_data values (one per exited child in this batch).
    static std::vector<uint64_t> wait_any();

    // Called in each child after fork.
    // Creates a new ring attached to the anchor (ATTACH_WQ).
    // Falls back to plain ring if anchor unavailable.
    static int create_child_ring();

    // True if io_uring is available and anchor was initialised.
    static bool available();

    // fd of anchor ring (needed by Lance FFI for ATTACH_WQ, optional).
    static int anchor_fd();

private:
    static int      anchor_fd_;
    static uint32_t calibrated_qd_;   // sysfs nr_requests/2, clamped [8, 128]
};
```

`wait_any()` calls `io_uring_enter(ring, 0, 1, IORING_ENTER_GETEVENTS)` then drains
the CQ with `io_uring_peek_batch_cqe`. It does **not** call `waitpid` — the caller
is responsible for reaping via `waitid(P_PIDFD, ...)` using the returned user_data
to look up the correct pidfd.

Queue-depth calibration is the same algorithm as `io_uring_store.rs`:
`stat(output_dir)` → `st_dev` → walk `/sys/block/*/dev` → read `queue/nr_requests` →
`nr_requests / 2`, clamped to `[8, 128]`.

### `IoUringOutputStream` — format-agnostic Arrow stream

```cpp
// include/tpch/io_uring_output_stream.hpp
class IoUringOutputStream : public arrow::io::OutputStream {
public:
    // ring_fd: io_uring fd created by IoUringPool::create_child_ring()
    // Spawns worker thread that owns the ring for the file's lifetime.
    IoUringOutputStream(const std::string& path, int ring_fd);

    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Flush() override;   // waits for all in-flight CQEs
    arrow::Status Close() override;  // Flush() + join worker thread + close fd

    arrow::Result<int64_t> Tell() const override;
    bool closed() const override;

private:
    struct WriteJob {
        std::vector<uint8_t> data;
        int64_t offset;
        std::promise<arrow::Status> done;
    };

    std::atomic<int64_t> write_offset_{0};   // lock-free offset pre-claiming
    std::thread worker_;
    // MPSC: producer = Arrow caller, consumer = worker thread
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<WriteJob> queue_;
    bool closed_ = false;
};
```

Worker thread loop (mirrors `uring_write` in `io_uring_store.rs`):

```
loop:
    job = queue.pop_blocking()
    for chunk in job.data (chunk_size = 512 KB):
        fill SQ up to ring QD
        if SQ full: submit_and_wait(1), drain CQEs
    submit_and_wait(in_flight)   // drain remaining
    job.done.set_value(ok)
```

No SQPOLL. No O_DIRECT. 512 KB SQE chunks. Same rules as Rust implementation.

### Writer factory injection

Writers already accept an `arrow::io::OutputStream` for initialisation (Parquet, CSV).
The factory function changes from:

```cpp
// Before
ARROW_ASSIGN_OR_RAISE(stream, arrow::io::FileOutputStream::Open(path));
```

to:

```cpp
// After — when io_uring is available (parallel mode)
if (IoUringPool::available()) {
    int ring_fd = IoUringPool::create_child_ring();
    stream = std::make_shared<IoUringOutputStream>(path, ring_fd);
} else {
    ARROW_ASSIGN_OR_RAISE(stream, arrow::io::FileOutputStream::Open(path));
}
```

**Format writers have zero knowledge of io_uring.** The injection point is
`create_writer()` in `tpcds_main.cpp`, not inside each writer class.

### Lance integration

Lance already has its own Rust-side io_uring (`io_uring_store.rs`) with an internal
`ANCHOR` lazy_static. When Lance runs in a child process that inherited the C++ anchor
fd, the Rust `ANCHOR` creates a separate pool by default.

To share one pool: add a Rust FFI function that accepts the parent's anchor fd and
uses it as `wq_fd` for all Lance rings:

```rust
// third_party/lance-ffi/src/lib.rs
#[no_mangle]
pub extern "C" fn lance_writer_attach_io_uring_pool(anchor_fd: i32) { … }
```

Called from `tpcds_main.cpp` in each child before the Lance writer is constructed.
This is **optional** — Lance works correctly without it; sharing just avoids spawning
a second set of kernel async-worker threads.

---

## DSDGen Fork Safety

### The `tmp_dist_path_` problem

`DSDGenWrapper::init_dsdgen()` writes a temp file (`/tmp/tpcds_idx_XXXXXX`) and stores
its path in `tmp_dist_path_`. The destructor calls `unlink(tmp_dist_path_)`.
After fork, every child inherits the same path. The first child to exit deletes the
file. This is benign (all children already loaded distributions into memory via
`init_rand()` before fork) but causes spurious `unlink` failures for later children.

**Fix**: add two methods to `DSDGenWrapper`:

```cpp
void set_skip_init(bool skip);     // child: skip init_dsdgen() entirely
void clear_tmp_path();             // child: clear path so destructor won't unlink
```

Child pattern after fork:

```cpp
// In child process, immediately after fork():
dsdgen_child.set_skip_init(true);
dsdgen_child.clear_tmp_path();
```

The parent (not any child) unlinks the temp file after `waitpid` for all children.

### Seed isolation

TPC-DS dsdgen uses `Streams[]` partitioned by table ID (same design as TPC-H `Seed[]`).
`init_rand()` initialises all streams; each table's generation reads only its own
stream slots. Children never touch sibling streams. Fork-after-init is safe.

**Required verification** (after DS-10.1 implementation):

```bash
# Sequential reference
./tpcds_benchmark --format parquet --table store_sales --sf 1 --max-rows 0
# Record row count per table

# Parallel run
./tpcds_benchmark --parallel --format parquet --sf 1 --max-rows 0
# Verify each table's row count matches the sequential reference
```

---

## CLI Changes

```
tpcds_benchmark [existing flags] [new flags]

  --parallel               Generate all implemented tables in parallel.
                           Activates io_uring (if available) automatically.
  --parallel-tables <N>    Limit concurrency to N tables at a time
                           (default: all tables; useful on memory-constrained hosts).
```

No `--lance-io-uring`, no `--parquet-io-uring` — the I/O backend is an
implementation detail of `--parallel`.

---

## Implementation Phases

### DS-10.1 — Fork-after-init parallel generation

**Files**: `src/tpcds_main.cpp`, `include/tpch/dsdgen_wrapper.hpp`,
`src/dsdgen/dsdgen_wrapper.cpp`
**New lines**: ~165

Steps:
1. Add `set_skip_init(bool)` and `clear_tmp_path()` to `DSDGenWrapper`.
2. Add `Options::parallel` and `Options::parallel_tables` fields + getopt parsing.
3. Implement `generate_all_tables_parallel(const Options&)`:
   - Build the canonical table list (all 24 implemented tables).
   - Init one `DSDGenWrapper` in the parent; call `init_dsdgen()`.
   - Fork loop: each child calls `set_skip_init` + `clear_tmp_path`, creates its own
     writer, generates its table, exits.
   - Parent: `waitpid(-1, &status, 0)` loop; unlinks temp dist file after all children
     finish; returns non-zero if any child failed.
4. Wire into `main()`: if `opts.parallel`, call `generate_all_tables_parallel`.

### DS-10.2 — `IoUringPool` + `IoUringOutputStream`

**Files**: `include/tpch/io_uring_pool.hpp`, `src/io/io_uring_pool.cpp`,
`include/tpch/io_uring_output_stream.hpp`, `src/io/io_uring_output_stream.cpp`
**New lines**: ~300
**Build gate**: `TPCH_ENABLE_ASYNC_IO` (already exists); stubs compile to
`FileOutputStream` fallback when flag is off.

Steps:
1. `IoUringPool::init_anchor(output_dir)`:
   - `sysfs_queue_depth(output_dir)` → calibrate QD.
   - `io_uring_setup(qd, 0)` → `anchor_fd_`.
2. `IoUringPool::create_child_ring()`:
   - `io_uring_setup(qd, IORING_SETUP_ATTACH_WQ, .wq_fd = anchor_fd_)`.
   - Falls back to plain ring if `anchor_fd_ == -1`.
3. `IoUringOutputStream`:
   - Constructor: open file `O_WRONLY|O_CREAT|O_TRUNC`; spawn worker thread.
   - `Write()`: atomically pre-claim offset; enqueue `WriteJob` to MPSC; block
     until `done` future resolves.
   - Worker loop: pop jobs, fill SQ with 512 KB SQEs, `io_uring_submit_and_wait`,
     drain CQEs, resolve futures.
   - `Close()`: send sentinel; join worker; close file fd.

### DS-10.3 — Inject stream into `create_writer()` factory

**Files**: `src/tpcds_main.cpp` (mainly), `src/writers/parquet_writer.cpp`,
`src/writers/csv_writer.cpp`
**New lines**: ~50

`create_writer()` gains an optional `std::shared_ptr<arrow::io::OutputStream>` parameter.
When provided (parallel mode + io_uring available), writers use it directly instead of
opening their own `FileOutputStream`.
Writers themselves do not change; the injection is in the factory.

### DS-10.4 — Lance: share kernel worker pool (optional)

**Files**: `third_party/lance-ffi/src/lib.rs`, `include/tpch/lance_ffi.h`,
`src/tpcds_main.cpp`
**New lines**: ~30 Rust + 10 C++

Add `lance_writer_attach_io_uring_pool(anchor_fd: i32)` FFI.
Called in each child before the Lance writer is created; routes all Lance rings through
the C++ anchor's kernel worker pool.

---

## Expected Performance

### DS-10.1 alone (parallel generation, no io_uring)

All 24 tables overlap on the VirtIO-blk queue naturally — no io_uring needed.
Total wall time ≈ time of the slowest table (likely `store_sales` or `catalog_sales`).

| SF | Sequential (est.) | Parallel (est.) | Speedup |
|----|-------------------|-----------------|---------|
| 1  | ~90 s             | ~15 s           | ~6×     |
| 5  | ~450 s            | ~70 s           | ~6×     |
| 10 | ~1800 s           | ~150 s          | ~12×    |

### DS-10.1 + DS-10.2 (parallel + io_uring)

Each child's writes are pipelined via `ATTACH_WQ` → shared kernel pool drains faster.
Based on Lance io_uring experiments (+2.15× per table), combined effect:

| SF | Parallel only | Parallel + io_uring | Total vs sequential |
|----|---------------|---------------------|---------------------|
| 10 | ~150 s        | ~70 s (est.)        | ~25×                |

---

## Fallback Strategy

| Condition | Behaviour |
|-----------|-----------|
| Kernel < 5.1 (no io_uring) | `IoUringPool::available()` returns false; `FileOutputStream` used |
| `TPCH_ENABLE_ASYNC_IO=OFF` | Stubs compile; `--parallel` still works, just without io_uring |
| io_uring_setup fails at runtime | Log warning; fall back to `FileOutputStream` |
| Lance without `attach_io_uring_pool` | Lance uses its own independent anchor; correct, slightly less efficient |

---

## File Map

```
include/tpch/
  io_uring_pool.hpp          ← new (DS-10.2)
  io_uring_output_stream.hpp ← new (DS-10.2)
  dsdgen_wrapper.hpp         ← add set_skip_init / clear_tmp_path (DS-10.1)

src/
  tpcds_main.cpp             ← parallel fork loop + create_writer injection (DS-10.1, DS-10.3)
  io/
    io_uring_pool.cpp        ← new (DS-10.2)
    io_uring_output_stream.cpp ← new (DS-10.2)
  dsdgen/
    dsdgen_wrapper.cpp       ← set_skip_init / clear_tmp_path impl (DS-10.1)
  writers/
    parquet_writer.cpp       ← accept injected stream (DS-10.3, minimal)
    csv_writer.cpp           ← accept injected stream (DS-10.3, minimal)

third_party/lance-ffi/src/
  lib.rs                     ← lance_writer_attach_io_uring_pool FFI (DS-10.4)

CMakeLists.txt               ← add src/io/*.cpp to tpch_core sources
```

---

## Open Questions

1. **Rolling fork vs batch**: The `pidfd` + `POLL_ADD` scheduler loop is inherently
   rolling — a new child is forked immediately when a CQE fires, keeping N slots
   busy at all times. No separate "batch" vs "rolling" decision is needed. ✅ Resolved.

2. **Lance `--zero-copy` in parallel mode**: Each child uses its own Lance writer
   independently. Streaming mode (`zero_copy_mode=async`) spawns a Tokio runtime per
   child. With 24 children this is 24 Tokio runtimes — acceptable since they are in
   separate processes (no shared address space).

3. **`max_rows` per child**: In parallel mode `--max-rows 0` should be the default
   (generating partial tables in parallel is only useful for benchmarking). Consider
   warning if `--parallel` is used without `--max-rows 0`.

4. **Output file collision**: All children write to `output_dir/<table>.<ext>`.
   Since table names are unique, no collision is possible. Document explicitly.

5. **pidfd reuse slot vs linear array**: The scheduler loop reuses the `idx` slot
   (user_data) of the completed child for the next fork. This requires a fixed-size
   array of `pidfds[N]`. Simpler than a free-list; correct because exactly one slot
   frees per CQE before being refilled.
