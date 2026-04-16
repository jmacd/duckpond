// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Build script: captures git SHA for build identification in sitegen footer.

use std::process::Command;

fn main() {
    // Get git short SHA
    let git_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=DUCKPOND_GIT_SHA={}", git_sha);

    // Only rerun if git HEAD changes
    println!("cargo:rerun-if-changed=../../.git/HEAD");
}
