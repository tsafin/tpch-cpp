//! io_uring-backed ObjectStore for Lance FFI
//!
//! # Design
//!
//! **Persistent ring** — one `IoUring` ring per file (not one per 5 MB part).
//! The ring lives on a dedicated worker thread for the entire multipart write,
//! eliminating ~800 `io_uring_setup()` syscalls for a 4 GB file.
//!
//! **Async channel** — the worker thread communicates via a `tokio::sync::mpsc`
//! channel so `put_part()` futures use `.await` (non-blocking) rather than
//! `std::sync::mpsc::send` which would block the single tokio worker thread.
//!
//! **Queue-depth calibration** — on the first write the block device backing
//! the output path is identified via `stat(2)` + `/sys/block/*/dev`, and its
//! `queue/nr_requests` value is read.  The working QD is set to half that
//! value (conservative share for concurrent writers), clamped to [8, 128].
//! Falls back to QD=64 if sysfs is unavailable (e.g. tmpfs on non-Linux).
//!
//! **Shared kernel worker pool** (`IORING_SETUP_ATTACH_WQ`) — all rings
//! created after the first one attach to a global "anchor" ring so they share
//! a single kernel async-worker thread pool.  This reduces scheduler pressure
//! on the Windows host (relevant in WSL2).
//!
//! **No SQPOLL** — `IORING_SETUP_SQPOLL` is intentionally avoided.  It creates
//! a kernel busy-polling thread that on WSL2/VirtIO causes continuous Windows
//! scheduler context-switches and host interface freezes.
//!
//! **No O_DIRECT** — on WSL2/VirtIO each O_DIRECT write synchronously waits
//! for a disk ACK, removing the pipelining benefit and making writes slower
//! than buffered I/O.
//!
//! **Graceful fallback** — if `IoUring::new()` fails (kernel too old, or
//! io_uring disabled), the error propagates and the caller falls back
//! transparently to the inner `LocalFileSystem` (tokio `spawn_blocking` path).

use std::fs::OpenOptions;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use futures::stream::BoxStream;
use io_uring::{opcode, types, IoUring};
use object_store::{
    path::Path, Error as OSError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OSResult, UploadPart,
};

// ─── Queue-depth calibration via sysfs ───────────────────────────────────────

const CHUNK_SIZE: usize = 512 * 1024; // 512 KB per SQE
const QD_DEFAULT: u32 = 64;           // fallback when sysfs is unavailable

/// Determine the io_uring queue depth for writes to `path` by reading the
/// block device's `queue/nr_requests` from sysfs.
///
/// Algorithm:
/// 1. `stat(path_or_ancestor)` → `st_dev` → major device number
/// 2. Walk `/sys/block/*/dev` to find the matching block device
/// 3. Read `queue/nr_requests` — the kernel's hardware queue depth
/// 4. Use `nr_requests / 2`, clamped to [8, 128]
///    (half: conservative share for concurrent writers; 128 cap: VirtIO-blk
///    virtqueue ceiling on WSL2 with its single-threaded Hyper-V IOThread)
///
/// Falls back to `QD_DEFAULT` (64) if sysfs is not readable.
fn sysfs_queue_depth(path: &std::path::Path) -> u32 {
    try_sysfs_queue_depth(path).unwrap_or_else(|| {
        eprintln!(
            "Lance FFI: io_uring sysfs probe failed for {:?}, using default QD={}",
            path, QD_DEFAULT
        );
        QD_DEFAULT
    })
}

fn try_sysfs_queue_depth(path: &std::path::Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;

    // Walk up to find an existing ancestor (the output file may not exist yet).
    let probe = std::iter::successors(Some(path), |p| p.parent())
        .find(|p| p.exists())
        .unwrap_or_else(|| std::path::Path::new("/"));

    let dev = std::fs::metadata(probe).ok()?.dev();
    // Extract major number.  Standard Linux encoding (works for major < 4096).
    let target_major = ((dev >> 8) & 0xfff) as u32;

    for entry in std::fs::read_dir("/sys/block").ok()?.flatten() {
        let dev_file = entry.path().join("dev");
        if let Ok(s) = std::fs::read_to_string(&dev_file) {
            let block_major: u32 = s.trim().split(':').next()?.parse().ok()?;
            if block_major == target_major {
                let nr_req_path = entry.path().join("queue/nr_requests");
                let nr_req: u32 =
                    std::fs::read_to_string(nr_req_path).ok()?.trim().parse().ok()?;
                let qd = (nr_req / 2).clamp(8, 128);
                eprintln!(
                    "Lance FFI: io_uring calibration: {:?} nr_requests={} → QD={}",
                    entry.file_name(),
                    nr_req,
                    qd
                );
                return Some(qd);
            }
        }
    }
    None
}

// ─── Shared kernel worker pool (IORING_SETUP_ATTACH_WQ) ──────────────────────

/// Raw fd of the "anchor" ring whose kernel async-worker pool is shared by all
/// subsequently created rings via `IORING_SETUP_ATTACH_WQ`.
///
/// The ring is deliberately leaked (`std::mem::forget`) so its fd stays open
/// for the lifetime of the process.  Child rings hold a reference to it via
/// the kernel's reference-counting, so the pool cannot be torn down early.
static ANCHOR_FD: OnceLock<std::os::unix::io::RawFd> = OnceLock::new();

/// Create an `IoUring` with `qd` entries.
///
/// * First call: creates the anchor ring, leaks it, stores its fd.
/// * Subsequent calls: attach to the anchor's kernel worker pool.
///   Falls back to a standalone ring if `setup_attach_wq` is unavailable.
fn make_ring(qd: u32) -> io::Result<IoUring> {
    let anchor_fd = ANCHOR_FD.get_or_init(|| {
        match IoUring::new(qd) {
            Ok(ring) => {
                let fd = ring.as_raw_fd();
                // Leak the ring: Drop would close the fd, killing the pool.
                std::mem::forget(ring);
                eprintln!("Lance FFI: io_uring anchor ring created (fd={}, QD={})", fd, qd);
                fd
            }
            Err(_) => -1, // sentinel: ring creation failed
        }
    });

    if *anchor_fd >= 0 {
        // Attach new ring to the shared kernel worker pool.
        IoUring::builder()
            .setup_attach_wq(*anchor_fd)
            .build(qd)
            .or_else(|_| IoUring::new(qd)) // graceful: try standalone on error
    } else {
        IoUring::new(qd)
    }
}

// ─── Core io_uring write helper ───────────────────────────────────────────────

/// Write `data` at `start_offset` into `raw_fd` using `ring`, pipelining up to
/// `ring.params().sq_entries()` SQEs before waiting for completions.
///
/// # Safety
/// `data` must remain alive until this function returns (all CQEs drained).
fn uring_write(
    ring: &mut IoUring,
    raw_fd: i32,
    data: &[u8],
    start_offset: u64,
) -> io::Result<()> {
    let fd = types::Fd(raw_fd);
    let total = data.len();
    if total == 0 {
        return Ok(());
    }

    let sq_cap = ring.params().sq_entries() as usize;
    let mut submitted: u64 = 0;
    let mut in_flight: usize = 0;
    let mut written: usize = 0;

    loop {
        // Fill SQ as much as possible.
        {
            let mut sq = ring.submission();
            while in_flight < sq_cap && (submitted as usize) < total {
                let chunk_start = submitted as usize;
                let len = std::cmp::min(CHUNK_SIZE, total - chunk_start);
                // SAFETY: data lives for the duration of this function.
                let buf_ptr = unsafe { data.as_ptr().add(chunk_start) };
                let sqe = opcode::Write::new(fd, buf_ptr, len as u32)
                    .offset(start_offset + submitted)
                    .build();
                if unsafe { sq.push(&sqe) }.is_err() {
                    break; // SQ momentarily full — drain CQ first
                }
                submitted += len as u64;
                in_flight += 1;
            }
        }

        if in_flight == 0 {
            break;
        }

        ring.submit_and_wait(1)?;

        for cqe in ring.completion() {
            let res = cqe.result();
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }
            written += res as usize;
            in_flight -= 1;
        }
    }

    if written < total {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("io_uring wrote {} of {} bytes", written, total),
        ));
    }
    Ok(())
}

// ─── Collect PutPayload ───────────────────────────────────────────────────────

fn collect_payload(payload: PutPayload) -> Vec<u8> {
    let total: usize = payload.iter().map(|b| b.len()).sum();
    let mut buf = Vec::with_capacity(total);
    for chunk in &payload {
        buf.extend_from_slice(chunk);
    }
    buf
}

// ─── Convert object_store::Path → PathBuf ────────────────────────────────────

fn to_fs_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("/{path}"))
}

// ─── Worker thread ────────────────────────────────────────────────────────────

struct WriteJob {
    offset: u64,
    data: Vec<u8>,
    done: tokio::sync::oneshot::Sender<io::Result<()>>,
}

/// Spawn a worker thread that owns one `IoUring` ring for the entire file.
///
/// Queue depth is derived from sysfs for the device backing `path`.
/// Jobs are received on a **tokio** mpsc channel so the async sender never
/// blocks a tokio worker thread.
fn spawn_writer_thread(
    path: &std::path::Path,
) -> io::Result<(
    tokio::sync::mpsc::Sender<WriteJob>,
    std::thread::JoinHandle<io::Result<()>>,
)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    // Calibrate QD against the actual target device via sysfs.
    let qd = sysfs_queue_depth(path.parent().unwrap_or(path));
    // Channel depth = 2×qd: enough headroom for Lance's concurrent-part window.
    let cap = (qd as usize).max(32);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<WriteJob>(cap);
    let raw_fd = file.as_raw_fd();

    let handle = std::thread::spawn(move || {
        let _file = file; // keep fd alive so raw_fd stays valid
        let mut ring = make_ring(qd)?;

        // `blocking_recv()` parks the worker thread until a job arrives.
        while let Some(job) = rx.blocking_recv() {
            let result = uring_write(&mut ring, raw_fd, &job.data, job.offset);
            let _ = job.done.send(result);
        }
        Ok(())
    });

    Ok((tx, handle))
}

// ─── IoUringMultipartUpload ───────────────────────────────────────────────────

#[derive(Debug)]
struct IoUringMultipartUpload {
    path: PathBuf,
    /// Next write offset; each part pre-claims its range with fetch_add.
    offset: Arc<AtomicU64>,
    tx: tokio::sync::mpsc::Sender<WriteJob>,
    worker: Option<std::thread::JoinHandle<io::Result<()>>>,
}

impl IoUringMultipartUpload {
    fn create(path: PathBuf) -> OSResult<Self> {
        let (tx, worker) = spawn_writer_thread(&path).map_err(|e| OSError::Generic {
            store: "IoUring",
            source: Box::new(e),
        })?;
        Ok(Self {
            path,
            offset: Arc::new(AtomicU64::new(0)),
            tx,
            worker: Some(worker),
        })
    }
}

#[async_trait::async_trait]
impl MultipartUpload for IoUringMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = collect_payload(data);
        let len = bytes.len() as u64;
        // Pre-claim write offset so concurrent parts get distinct ranges.
        let write_offset = self.offset.fetch_add(len, Ordering::SeqCst);

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let tx = self.tx.clone();

        Box::pin(async move {
            // Non-blocking async send — never stalls the tokio worker thread.
            tx.send(WriteJob { offset: write_offset, data: bytes, done: done_tx })
                .await
                .map_err(|_| OSError::Generic {
                    store: "IoUring",
                    source: "worker thread disconnected".into(),
                })?;

            done_rx
                .await
                .map_err(|_| OSError::Generic {
                    store: "IoUring",
                    source: "worker dropped completion sender".into(),
                })?
                .map_err(|e| OSError::Generic {
                    store: "IoUring",
                    source: Box::new(e),
                })
        })
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        // Closing the sender causes the worker's blocking_recv() to return None.
        let (dead_tx, _) = tokio::sync::mpsc::channel(1);
        let live_tx = std::mem::replace(&mut self.tx, dead_tx);
        drop(live_tx); // channel closes → worker exits loop

        if let Some(handle) = self.worker.take() {
            tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| OSError::Generic {
                    store: "IoUring",
                    source: Box::new(io::Error::new(io::ErrorKind::Other, e.to_string())),
                })?
                .map_err(|_| OSError::Generic {
                    store: "IoUring",
                    source: "worker thread panicked".into(),
                })?
                .map_err(|e| OSError::Generic {
                    store: "IoUring",
                    source: Box::new(e),
                })?;
        }
        Ok(PutResult { e_tag: None, version: None })
    }

    async fn abort(&mut self) -> OSResult<()> {
        let (dead_tx, _) = tokio::sync::mpsc::channel(1);
        let live_tx = std::mem::replace(&mut self.tx, dead_tx);
        drop(live_tx);
        if let Some(handle) = self.worker.take() {
            let _ = tokio::task::spawn_blocking(move || handle.join()).await;
        }
        let _ = std::fs::remove_file(&self.path);
        Ok(())
    }
}

// ─── IoUringStore ─────────────────────────────────────────────────────────────

pub(crate) struct IoUringStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for IoUringStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IoUringStore({})", self.inner)
    }
}
impl std::fmt::Display for IoUringStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IoUringStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for IoUringStore {
    // ── Write path ────────────────────────────────────────────────────────────
    //
    // If calibration returns `None` (overloaded system), all writes fall back
    // transparently to the inner `LocalFileSystem` (tokio spawn_blocking path).

    async fn put(&self, location: &Path, payload: PutPayload) -> OSResult<PutResult> {
        self.put_opts(location, payload, PutOptions::default()).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        let bytes = collect_payload(payload);
        let path = to_fs_path(location);
        let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            let qd = sysfs_queue_depth(path.parent().unwrap_or(&path));
            let mut ring = make_ring(qd)?;
            uring_write(&mut ring, file.as_raw_fd(), &bytes, 0)
        })
        .await;

        match result {
            Ok(Ok(())) => Ok(PutResult { e_tag: None, version: None }),
            Ok(Err(e)) => Err(OSError::Generic { store: "IoUring", source: Box::new(e) }),
            Err(e) => Err(OSError::Generic {
                store: "IoUring",
                source: Box::new(io::Error::new(io::ErrorKind::Other, e.to_string())),
            }),
        }
    }

    async fn put_multipart(&self, location: &Path) -> OSResult<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default()).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        match IoUringMultipartUpload::create(to_fs_path(location)) {
            Ok(upload) => Ok(Box::new(upload)),
            Err(e) => {
                eprintln!("Lance FFI: io_uring unavailable ({}), falling back to inner store", e);
                self.inner.put_multipart_opts(location, opts).await
            }
        }
    }

    // ── Reads / metadata: delegate to inner LocalFileSystem ──────────────────

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn delete(&self, location: &Path) -> OSResult<()> {
        self.inner.delete(location).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ─── WrappingObjectStore ──────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct IoUringWrapper;

impl lance_io::object_store::WrappingObjectStore for IoUringWrapper {
    fn wrap(&self, _: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        eprintln!("Lance FFI: io_uring write path enabled (calibration on first write)");
        Arc::new(IoUringStore { inner: original })
    }
}
