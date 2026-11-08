//! Clock skew in containers with `libfaketime` (chapter 46: "clock skew
//! with `libfaketime`").
//!
//! **Why not faultd.** Containers share the Docker VM's kernel clock (on
//! macOS, Docker Desktop's Linux VM). Linux time namespaces virtualise only
//! `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME`, not the wall clock, and apply
//! only to processes started inside them, so `faultd` (which works by
//! entering a running container's namespaces) can't move one container's
//! wall clock. Setting the clock from a privileged container
//! (`settimeofday`, `date -s`) moves the VM's clock for every container,
//! the shared Compose servers and etcd included, until Docker Desktop
//! resyncs it with the host. The per-container way is `libfaketime`
//! preloaded into the target process, which needs the image and the
//! environment to change: so only throwaway containers get it, never the
//! shared stack.
//!
//! **How.** [`ensure_image`] builds `sprigdb-mariadb-faketime:<tag>` once,
//! from a MariaDB image plus Ubuntu's `libfaketime` package (the build needs
//! the network), with `LD_PRELOAD` and `FAKETIME_TIMESTAMP_FILE` set, so
//! every process in the container reads its offset from [`RC_FILE`].
//! [`crate::faults::compose::OwnMariaDb::start_with_faketime`] runs it as
//! a `sprig-sim-*` container; [`set`] (and
//! [`ComposeFaults::skew_clock`](crate::faults::ComposeFaults)) write the
//! offset. A running `mariadbd` re-reads the file at most once a second
//! (`FAKETIME_CACHE_DURATION=1`); [`set`] waits [`SETTLE`] for that. The
//! monotonic clock isn't faked (`FAKETIME_DONT_FAKE_MONOTONIC=1`), so
//! timeouts and `InnoDB`'s timed waits behave.
//!
//! **Caveats.** Clients inside the container (`docker exec ... mariadb`)
//! are faked too: with the clock moved back, they reject the server's
//! auto-generated TLS certificate as "not yet valid" (use `--skip-ssl`).
//! Clients outside aren't affected. A rate (`rate_ppm`) is libfaketime's
//! speed factor, measured from when the offset was set; it is approximate.

use std::fmt::Write as _;
use std::io::Write as _;
use std::process::{Command, Stdio};
use std::time::Duration;

use sprig_common::{Result, SprigError};

use super::compose::{docker, ensure_own};

/// The offset file inside a faketime container.
pub const RC_FILE: &str = "/var/lib/sprig-faketime/rc";

/// How long [`set`] waits for a running process to pick up a new offset:
/// libfaketime's one-second cache, plus a margin.
pub const SETTLE: Duration = Duration::from_millis(2500);

/// The faketime image's repository; the tag is the base image's tag.
pub const IMAGE_REPO: &str = "sprigdb-mariadb-faketime";

/// The image: a MariaDB image with `libfaketime` preloaded in every
/// process.
pub const DOCKERFILE: &str = r#"ARG BASE=mariadb:11.8
FROM ${BASE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends libfaketime \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dpkg -L libfaketime | grep '/libfaketime\.so\.1$')" /usr/local/lib/libfaketime.so.1 \
 && mkdir -p /var/lib/sprig-faketime \
 && echo '+0' > /var/lib/sprig-faketime/rc \
 && chmod 777 /var/lib/sprig-faketime && chmod 666 /var/lib/sprig-faketime/rc
ENV LD_PRELOAD=/usr/local/lib/libfaketime.so.1 \
    FAKETIME_TIMESTAMP_FILE=/var/lib/sprig-faketime/rc \
    FAKETIME_CACHE_DURATION=1 \
    FAKETIME_DONT_FAKE_MONOTONIC=1
LABEL sprig-faketime=1
"#;

/// The faketime image for `base` (such as `mariadb:12.3`).
#[must_use]
pub fn image_for(base: &str) -> String {
    let tag = base.rsplit_once(':').map_or("latest", |(_, t)| t);
    format!("{IMAGE_REPO}:{tag}")
}

/// Builds the faketime image for `base` unless it exists; returns its name.
///
/// # Errors
/// If docker fails, notably when the build can't reach Ubuntu's mirrors.
pub fn ensure_image(base: &str) -> Result<String> {
    let image = image_for(base);
    if docker(&["image", "inspect", "--format", "{{.Id}}", &image]).is_ok() {
        return Ok(image);
    }
    let build_arg = format!("BASE={base}");
    let mut child = Command::new("docker")
        .args(["build", "-q", "-t", &image, "--build-arg", &build_arg, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SprigError::internal("running docker build").with_source(e))?;
    child
        .stdin
        .take()
        .ok_or_else(|| SprigError::internal("docker build: no stdin"))?
        .write_all(DOCKERFILE.as_bytes())
        .map_err(|e| SprigError::internal("writing the Dockerfile").with_source(e))?;
    let out = child
        .wait_with_output()
        .map_err(|e| SprigError::internal("waiting for docker build").with_source(e))?;
    if out.status.success() {
        Ok(image)
    } else {
        Err(SprigError::internal(format!(
            "docker build {image}: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

/// libfaketime's relative spec for an offset and a rate: `+1.500s`,
/// `-30.000s x1.000100`.
#[must_use]
pub fn spec(offset_ms: i64, rate_ppm: i64) -> String {
    let sign = if offset_ms < 0 { '-' } else { '+' };
    let abs = offset_ms.unsigned_abs();
    let mut s = format!("{sign}{}.{:03}s", abs / 1000, abs % 1000);
    if rate_ppm != 0 {
        #[allow(clippy::cast_precision_loss)] // ppm values are small
        let factor = 1.0 + rate_ppm as f64 / 1_000_000.0;
        let _ = write!(s, " x{factor:.6}");
    }
    s
}

/// Whether `container` was started from a faketime image.
///
/// # Errors
/// If `container` isn't an own container or docker fails.
pub fn is_faketime(container: &str) -> Result<bool> {
    ensure_own(container)?;
    let env = docker(&[
        "inspect",
        "-f",
        "{{range .Config.Env}}{{println .}}{{end}}",
        container,
    ])?;
    Ok(env
        .lines()
        .any(|l| l == format!("FAKETIME_TIMESTAMP_FILE={RC_FILE}")))
}

/// Moves `container`'s wall clock by `offset_ms` (and its rate by
/// `rate_ppm`), then waits [`SETTLE`] so running processes see it.
/// `set(c, 0, 0)` undoes it.
///
/// # Errors
/// If `container` isn't an own faketime container or docker fails.
pub async fn set(container: &str, offset_ms: i64, rate_ppm: i64) -> Result<()> {
    if !is_faketime(container)? {
        return Err(SprigError::internal(format!(
            "{container} wasn't started with libfaketime \
             (OwnMariaDb::start_with_faketime); its clock can't be skewed"
        )));
    }
    let line = spec(offset_ms, rate_ppm);
    docker(&[
        "exec",
        container,
        "sh",
        "-c",
        &format!("printf '%s\\n' '{line}' > {RC_FILE}"),
    ])?;
    tokio::time::sleep(SETTLE).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specs_are_libfaketime_offsets() {
        assert_eq!(spec(0, 0), "+0.000s");
        assert_eq!(spec(1500, 0), "+1.500s");
        assert_eq!(spec(-30_000, 100), "-30.000s x1.000100");
        assert_eq!(spec(-5, -50), "-0.005s x0.999950");
    }

    #[test]
    fn images_follow_their_base() {
        assert_eq!(image_for("mariadb:12.3"), "sprigdb-mariadb-faketime:12.3");
        assert_eq!(image_for("mariadb"), "sprigdb-mariadb-faketime:latest");
    }

    #[tokio::test]
    async fn refuses_shared_containers() {
        assert!(set("sprigdb-dev-single-1", 1000, 0).await.is_err());
    }
}
