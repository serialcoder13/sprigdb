//! Declares `cfg(tokio_unstable)`, which the simulation uses to seed tokio's
//! own random choices (`select!` branch order) when the build enables it
//! (`RUSTFLAGS="--cfg tokio_unstable"`, as CI's simulation job does).

fn main() {
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
}
