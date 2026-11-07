//! The Compose fixture against a real Docker. Needs `SPRIG_TEST_DOCKER=1`.

use std::time::Duration;

use sprig_testkit::{ComposeProject, require_env, skip};

const HELLO: &str = "crates/sprig-testkit/tests/fixtures/hello.yaml";

#[test]
fn stack_starts_answers_and_tears_down_on_drop() {
    require_env!(skip::DOCKER);
    let stack = ComposeProject::new(HELLO)
        .wait_timeout(Duration::from_secs(120))
        .up()
        .unwrap();
    let name = stack.project().project_name().to_owned();

    let ps = stack.ps().unwrap();
    assert_eq!(ps.len(), 1);
    assert!(ps[0].is_ready(), "{ps:?}");
    stack.wait_healthy(Duration::from_secs(10)).unwrap();

    assert_eq!(stack.exec("hello", &["echo", "hi"]).unwrap().trim(), "hi");
    let addr = stack.port("hello", 8080).unwrap();
    assert!(addr.starts_with("127.0.0.1:"), "{addr}");
    assert!(stack.logs().unwrap().contains("hello from the fixture"));

    drop(stack);
    let after = ComposeProject::new(HELLO).name(name).attach();
    assert!(
        after.ps().unwrap().is_empty(),
        "project still has containers"
    );
}

#[test]
fn failed_start_reports_logs_and_cleans_up() {
    require_env!(skip::DOCKER);
    let project = ComposeProject::new(HELLO)
        .profile("broken")
        .wait_timeout(Duration::from_secs(60));
    let name = project.project_name().to_owned();
    let err = project.up().unwrap_err();
    assert!(
        err.message.contains("failing on purpose"),
        "the error should end with the logs: {}",
        err.message
    );
    if !skip::flag(skip::KEEP) {
        let after = ComposeProject::new(HELLO)
            .profile("broken")
            .name(name)
            .attach();
        assert!(
            after.ps().unwrap().is_empty(),
            "project still has containers"
        );
    }
}
