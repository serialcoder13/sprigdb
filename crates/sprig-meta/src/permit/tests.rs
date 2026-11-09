use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::time::Duration;

use sprig_common::time::Clock;

use super::*;
use crate::session::TokioClock;

/// Answers after `delay` with `valid_ms`, or refuses.
struct Fixed {
    delay: Duration,
    valid_ms: AtomicU32,
    fenced: AtomicBool,
    calls: AtomicUsize,
}

impl Fixed {
    fn new(delay: Duration, valid_ms: u32) -> Arc<Self> {
        Arc::new(Self {
            delay,
            valid_ms: AtomicU32::new(valid_ms),
            fenced: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl PermitSource for Fixed {
    async fn renew(&self, _req: &PermitRequest) -> Result<Permit> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if self.fenced.load(Ordering::SeqCst) {
            return Ok(Permit::fenced(3));
        }
        Ok(Permit {
            dc_epoch: 3,
            valid_ms: self.valid_ms.load(Ordering::SeqCst),
            fenced: false,
            admit_per_second: 50,
        })
    }
}

struct Down;

#[async_trait::async_trait]
impl PermitSource for Down {
    async fn renew(&self, _req: &PermitRequest) -> Result<Permit> {
        Err(SprigError::new(SprigCode::MetaUnavailable, "down"))
    }
}

fn holder() -> (PermitHolder, Arc<dyn Clock>) {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
    (
        PermitHolder::new("gw-1", "gateway", Arc::clone(&clock)),
        clock,
    )
}

#[tokio::test(start_paused = true)]
async fn the_permit_runs_from_the_send_time() {
    let (h, clock) = holder();
    assert!(!h.writable(), "no permit yet");
    let src: Arc<dyn PermitSource> = Fixed::new(Duration::from_millis(400), 5000);
    let sent = clock.now();
    assert!(h.renew_once(&[src], Duration::from_secs(1)).await);
    assert_eq!(h.expiry(), Some(sent + Duration::from_millis(5000)));
    assert_eq!(h.dc_epoch(), 3);
    assert_eq!(h.admit_per_second(), 50);
    tokio::time::sleep(Duration::from_millis(4500)).await;
    assert!(h.writable());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !h.writable(),
        "the 400 ms in flight count against the permit"
    );
}

#[tokio::test(start_paused = true)]
async fn a_refusal_ends_the_permit_at_once() {
    let (h, _clock) = holder();
    let f = Fixed::new(Duration::ZERO, 5000);
    let src: Arc<dyn PermitSource> = f.clone();
    assert!(
        h.renew_once(&[Arc::clone(&src)], Duration::from_secs(1))
            .await
    );
    f.fenced.store(true, Ordering::SeqCst);
    assert!(!h.renew_once(&[src], Duration::from_secs(1)).await);
    assert_eq!(h.remaining(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn the_next_source_answers_when_one_is_down() {
    let (h, _clock) = holder();
    let f = Fixed::new(Duration::ZERO, 5000);
    let srcs: Vec<Arc<dyn PermitSource>> = vec![Arc::new(Down), f.clone()];
    assert!(h.renew_once(&srcs, Duration::from_secs(1)).await);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn unreachable_controllers_let_the_permit_lapse() {
    let (h, _clock) = holder();
    let f = Fixed::new(Duration::ZERO, 5000);
    assert!(h.renew_once(&[f], Duration::from_secs(1)).await);
    let down: Vec<Arc<dyn PermitSource>> = vec![Arc::new(Down)];
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(h.renew_once(&down, Duration::from_secs(1)).await);
    }
    tokio::time::sleep(Duration::from_millis(1001)).await;
    assert!(!h.renew_once(&down, Duration::from_secs(1)).await);
}

#[test]
fn the_watchdog_fences_then_aborts_a_stuck_loop() {
    let clock: Arc<dyn Clock> = Arc::new(sprig_common::time::SystemClock);
    let h = Arc::new(PermitHolder::new("gw", "gateway", clock));
    let aborted = Arc::new(AtomicBool::new(false));
    let a = Arc::clone(&aborted);
    let dog = Watchdog::over(
        &h,
        Duration::from_millis(100),
        Box::new(move || a.store(true, Ordering::SeqCst)),
    );
    std::thread::sleep(Duration::from_millis(150));
    assert!(h.is_forced(), "stalled past watchdog_ms: fenced");
    assert!(!aborted.load(Ordering::SeqCst));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        aborted.load(Ordering::SeqCst),
        "stalled past twice that: abort"
    );
    drop(dog);
}

#[test]
fn a_ticking_loop_keeps_the_watchdog_quiet() {
    let clock: Arc<dyn Clock> = Arc::new(sprig_common::time::SystemClock);
    let h = Arc::new(PermitHolder::new("gw", "gateway", clock));
    let dog = Watchdog::over(&h, Duration::from_millis(60), Box::new(|| {}));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("rt");
    rt.block_on(async {
        for _ in 0..10 {
            h.renew_once(&[], Duration::from_millis(1)).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    assert!(!h.is_forced());
    drop(dog);
}
