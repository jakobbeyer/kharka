//! The commands that print a file: shell completions and the man page.

mod common;

use common::*;

fn print(args: &[&str]) -> String {
    let dir = tempfile::tempdir().unwrap();
    let output = kharka(dir.path()).args(args).output().unwrap();
    assert!(output.status.success(), "kharka {args:?} failed");
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn completions_name_the_command() {
    for shell in ["bash", "zsh", "fish"] {
        let script = print(&["completions", shell]);
        assert!(!script.is_empty(), "{shell}: nothing printed");
        assert!(script.contains("kharka"), "{shell}: no mention of kharka");
    }
}

#[test]
fn man_page_is_roff() {
    let page = print(&["man"]);
    assert!(page.contains(".TH"), "no title line");
    assert!(page.contains("kharka"));
}

#[test]
fn man_page_has_the_nested_commands() {
    let page = print(&["man"]);
    assert!(page.contains("collections set"), "no `collections set`");
    assert!(page.contains("collections rm"), "no `collections rm`");
    assert!(page.contains("\\-\\-schema"), "no --schema");
}

#[test]
fn unknown_shell_is_a_usage_error() {
    let dir = tempfile::tempdir().unwrap();
    let output = kharka(dir.path()).args(["completions", "cmd"]).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
}
