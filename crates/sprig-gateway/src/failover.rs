//! Chapter 25 in the gateway (R2): the DC permit, its watchdog, the write
//! gate and admission while the DC returns.
//!
//! - **Permit.** A [`PermitKeeper`] renews the gateway's permit every
//!   second from its DC's controllers (`controllers/<node>`'s
//!   `internal_addr`). It enforces only once armed: the first controller
//!   seen that serves permits arms it for good (`sprig-meta::permit::http`).
//! - **Writes.** A write (or `COMMIT`) of a tenant homed in this DC needs
//!   a valid permit, else 39501 `ER_SPRIG_DC_FENCED`; reads continue. A
//!   write whose followed map is `quarantined` (its copy here was fenced
//!   after a failover, and the new home isn't known yet) gets 39530
//!   `ER_SPRIG_RECOVERY_FENCED`, retryable after re-routing. The
//!   acknowledgement is checked too: a write that was sent with a valid
//!   permit but finished after it lapsed (the process was paused) is
//!   answered with 39501 instead of OK, so nothing is acknowledged by a
//!   fenced DC ([`GatedSink`]).
//! - **Watchdog.** A plain OS thread watches a heartbeat the runtime
//!   raises every 100 ms: a stall of `failover.watchdog_ms` fences the
//!   gateway directly, twice that aborts the process. A process that was
//!   paused as a whole (SIGSTOP, a frozen VM) only fences on resume.
//! - **Opting out.** With `failover.self_fence = false` for the DC, the
//!   gateway never refuses writes for a missing permit (chapter 25: such a
//!   DC skips self-fencing and never fails over automatically).
//! - **Admission.** While the DC returns from an outage, the permit
//!   carries the step's rate; new MySQL connections beyond it get 39531
//!   `ER_SPRIG_ADMISSION_BACKOFF` in place of the greeting, with a retry
//!   delay of 1-5 s, fully jittered; HTTP gets 503 with `Retry-After`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::ids::DcId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_meta::permit::http::PermitKeeper;
use sprig_meta::permit::{Admission, PermitHolder, Watchdog, dc_fenced_error};
use sprig_proto_mysql::packets::OkPacket;
use sprig_settings::{LiveSettings, SettingCtx};

use sprig_proto_mysql::ColumnDef;

use crate::pipeline::{EndOfRows, ResultSink, RowFormat, RowValues, Wire};

/// The renew period.
pub const RENEW_PERIOD: Duration = Duration::from_secs(1);

/// The gateway's chapter 25 state (see the module docs).
pub struct GatewayFence {
    dc: DcId,
    keeper: Arc<PermitKeeper>,
    live: Option<LiveSettings>,
    /// `failover.self_fence` for this DC, refreshed every 250 ms: `false`
    /// opts the DC out of self-fencing (and of automatic DC failover).
    self_fence: std::sync::atomic::AtomicBool,
    admission: Mutex<(u32, Admission)>,
    rate: AtomicU32,
    watchdog: Mutex<Option<Watchdog>>,
}

impl std::fmt::Debug for GatewayFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayFence")
            .field("dc", &self.dc)
            .field("keeper", &self.keeper)
            .finish_non_exhaustive()
    }
}

static FENCE: OnceLock<Arc<GatewayFence>> = OnceLock::new();

/// The process's fence, once [`start`] ran.
#[must_use]
pub fn get() -> Option<&'static Arc<GatewayFence>> {
    FENCE.get()
}

/// `failover.watchdog_ms` (chapter 25's key, declared by the controller).
fn watchdog_ms(live: &LiveSettings, dc: &DcId) -> Duration {
    let ms = live
        .resolve("failover.watchdog_ms", &SettingCtx::process(dc))
        .ok()
        .and_then(|r| match r.value {
            sprig_settings::Value::Int(n) => u64::try_from(n).ok(),
            _ => None,
        })
        .unwrap_or(1000);
    Duration::from_millis(ms.max(50))
}

/// Starts the permit loop, the watchdog and the metrics, and installs the
/// fence for the executor and the listeners. A second call returns the
/// first fence.
pub fn start(
    dc_store: Arc<dyn MetaStore>,
    dc: DcId,
    gateway_id: String,
    live: Option<&LiveSettings>,
    stop: CancellationToken,
) -> Arc<GatewayFence> {
    if let Some(f) = FENCE.get() {
        return Arc::clone(f);
    }
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let holder = Arc::new(PermitHolder::new(gateway_id, "gateway", clock));
    let keeper = Arc::new(PermitKeeper::new(Arc::clone(&holder)));
    let stall = live.map_or(Duration::from_secs(1), |l| watchdog_ms(l, &dc));
    let beat = keeper.heartbeat();
    let (k2, h2) = (Arc::clone(&keeper), Arc::clone(&holder));
    let watchdog = Watchdog::spawn(
        Arc::new(move || beat.load(Ordering::SeqCst)),
        stall,
        Box::new(move || {
            if k2.armed() {
                h2.force_fence();
            }
        }),
        {
            let k3 = Arc::clone(&keeper);
            Box::new(move || {
                if k3.armed() {
                    tracing::error!(
                        "the gateway's runtime is stuck: aborting (chapter 25 watchdog)"
                    );
                    std::process::abort();
                }
            })
        },
    );
    let fence = Arc::new(GatewayFence {
        dc,
        keeper: Arc::clone(&keeper),
        live: live.cloned(),
        self_fence: std::sync::atomic::AtomicBool::new(true),
        admission: Mutex::new((0, Admission::new(Instant::now(), seed()))),
        rate: AtomicU32::new(0),
        watchdog: Mutex::new(Some(watchdog)),
    });
    let fence = Arc::clone(FENCE.get_or_init(|| fence));
    tokio::spawn(Arc::clone(&keeper).run(dc_store, RENEW_PERIOD, stop.clone()));
    let f2 = Arc::clone(&fence);
    tokio::spawn(async move {
        while !stop.is_cancelled() {
            f2.report();
            tokio::select! {
                () = stop.cancelled() => {}
                () = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
        // Shutting down: the permit loop stops, so its heartbeat does too;
        // the watchdog must not take that for a stuck runtime and abort
        // the drain (r2next).
        let w = f2
            .watchdog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(w) = w {
            std::thread::spawn(move || drop(w));
        }
    });
    fence
}

fn seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |d| u64::from(d.subsec_nanos()) ^ d.as_secs())
        ^ u64::from(std::process::id())
}

impl GatewayFence {
    /// A fence for tests: not installed, permits from `keeper`.
    #[must_use]
    pub fn for_tests(dc: DcId, keeper: Arc<PermitKeeper>) -> Arc<Self> {
        Arc::new(Self {
            dc,
            keeper,
            live: None,
            self_fence: std::sync::atomic::AtomicBool::new(true),
            admission: Mutex::new((0, Admission::new(Instant::now(), 7))),
            rate: AtomicU32::new(0),
            watchdog: Mutex::new(None),
        })
    }

    /// This gateway's DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    /// The permit keeper.
    #[must_use]
    pub fn keeper(&self) -> &Arc<PermitKeeper> {
        &self.keeper
    }

    /// Whether writes of tenants homed here may proceed.
    #[must_use]
    pub fn may_write(&self) -> bool {
        !self.self_fence.load(Ordering::SeqCst) || self.keeper.may_write()
    }

    /// 39501 unless writes of tenants homed here may proceed.
    ///
    /// # Errors
    /// 39501 `ER_SPRIG_DC_FENCED`.
    pub fn check_write(&self) -> Result<()> {
        if self.may_write() {
            Ok(())
        } else {
            metrics::counter!("sprig_gateway_fenced_writes_total").increment(1);
            Err(dc_fenced_error(self.dc.as_str()))
        }
    }

    /// The admission rate in force (0: no limit).
    #[must_use]
    pub fn admission_rate(&self) -> u32 {
        if self.keeper.armed() {
            self.keeper.holder().admit_per_second()
        } else {
            0
        }
    }

    /// Admits a new client connection, or the retry delay of a 39531.
    ///
    /// # Errors
    /// The delay the client should wait.
    pub fn admit(&self, protocol: &'static str) -> std::result::Result<(), Duration> {
        let rate = self.admission_rate();
        if rate == 0 {
            return Ok(());
        }
        let mut a = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if a.0 == 0 {
            // A returning gateway starts with an empty bucket.
            a.1 = Admission::new(Instant::now(), seed());
        }
        a.0 = rate;
        let r = a.1.admit_or_wait(Instant::now(), rate);
        if r.is_err() {
            metrics::counter!("sprig_gateway_admission_backoffs_total", "protocol" => protocol)
                .increment(1);
        }
        r
    }

    fn report(&self) {
        if let Some(l) = &self.live {
            let on = l
                .resolve("failover.self_fence", &SettingCtx::process(&self.dc))
                .ok()
                .is_none_or(|r| !matches!(r.value, sprig_settings::Value::Bool(false)));
            self.self_fence.store(on, Ordering::SeqCst);
        }
        let h = self.keeper.holder();
        let armed = self.keeper.armed();
        metrics::gauge!("sprig_gateway_permit_remaining_seconds").set(h.remaining().as_secs_f64());
        metrics::gauge!("sprig_gateway_fenced").set(if armed && !h.writable() { 1.0 } else { 0.0 });
        let rate = self.admission_rate();
        self.rate.store(rate, Ordering::SeqCst);
        if rate == 0 {
            self.admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .0 = 0;
        }
        metrics::gauge!("sprig_gateway_admission_rate").set(f64::from(rate));
    }
}

/// 39530 `ER_SPRIG_RECOVERY_FENCED`.
#[must_use]
pub fn recovery_fenced(tenant: sprig_common::ids::TenantId) -> SprigError {
    SprigError::new(
        SprigCode::RecoveryFenced,
        format!(
            "tenant {tenant}'s copy in this data center is fenced after a failover; \
             retry: the gateway is finding its new home"
        ),
    )
}

/// The MySQL ERR packet a refused connection gets in place of the
/// greeting.
#[must_use]
pub fn admission_err_packet(wait: Duration) -> Vec<u8> {
    let e = SprigError::new(
        SprigCode::AdmissionBackoff,
        format!(
            "this data center is returning from an outage and admits connections in steps; \
             retry in {} ms",
            wait.as_millis()
        ),
    );
    sprig_proto_mysql::ErrPacket::new(SprigCode::AdmissionBackoff.number(), *b"08S01", &e.message)
        .encode(sprig_proto_mysql::Caps::empty())
        .to_vec()
}

/// A sink that turns the OK of a gated write into 39501 when the permit
/// lapsed while the write ran (see the module docs).
pub struct GatedSink<'a> {
    inner: &'a mut dyn ResultSink,
    fence: &'a GatewayFence,
    refused: bool,
}

impl<'a> GatedSink<'a> {
    /// Wraps `inner`.
    pub fn new(inner: &'a mut dyn ResultSink, fence: &'a GatewayFence) -> Self {
        Self {
            inner,
            fence,
            refused: false,
        }
    }
}

#[async_trait]
impl ResultSink for GatedSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        self.inner.begin_rows(columns, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        self.inner.begin_rows_raw(defs, format).await
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        self.inner.raw_row(payload).await
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        if !self.fence.may_write() {
            // The write ran past the permit: never acknowledged.
            self.refused = true;
            metrics::counter!("sprig_gateway_unacknowledged_writes_total").increment(1);
            tracing::warn!(dc = %self.fence.dc, "a write finished after the DC permit lapsed: answered 39501, not OK");
            let e = SprigError::new(
                SprigCode::DcFenced,
                format!(
                    "data center {} lost its write permit while the statement ran; its outcome is unknown",
                    self.fence.dc
                ),
            );
            return self.inner.error(&e).await;
        }
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        if self.refused {
            return Ok(());
        }
        self.inner.error(err).await
    }

    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    fn finished(&self) -> bool {
        self.refused || self.inner.finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_meta::permit::{Permit, PermitRequest, PermitSource};

    struct Fixed(Permit);

    #[async_trait]
    impl PermitSource for Fixed {
        async fn renew(&self, _r: &PermitRequest) -> Result<Permit> {
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn writes_need_a_permit_once_armed_and_admission_follows_its_rate() {
        let holder = Arc::new(PermitHolder::new("gw", "gateway", Arc::new(SystemClock)));
        let keeper = Arc::new(PermitKeeper::new(Arc::clone(&holder)));
        let f = GatewayFence::for_tests(DcId::new("fra1").unwrap(), Arc::clone(&keeper));
        assert!(f.check_write().is_ok(), "unarmed: as before R2");
        keeper.arm();
        assert_eq!(
            f.check_write().unwrap_err().code(),
            Some(SprigCode::DcFenced)
        );
        let mut p = Permit::valid(1, Duration::from_secs(5));
        p.admit_per_second = 50;
        let src: Vec<Arc<dyn PermitSource>> = vec![Arc::new(Fixed(p))];
        assert!(holder.renew_once(&src, Duration::from_secs(1)).await);
        assert!(f.check_write().is_ok());
        assert_eq!(f.admission_rate(), 50);
        // 300 connections at once: the bucket starts empty, then refills
        // at 50 per second.
        let refused = (0..300).filter(|_| f.admit("mysql").is_err()).count();
        assert!(refused >= 299, "refused {refused}");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let ok = (0..300).filter(|_| f.admit("mysql").is_ok()).count();
        assert!((5..=15).contains(&ok), "admitted {ok} after 200 ms at 50/s");
        let pkt = admission_err_packet(Duration::from_millis(1500));
        assert_eq!(&pkt[..3], &[0xff, 0x6b, 0x9a], "ERR 39531, little-endian");
    }
}
