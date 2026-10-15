// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![deny(rust_2018_idioms)]

#[macro_use]
extern crate log;

mod harness;

use assert_cmd::prelude::*;
use harness::{
    AdditionalStats, Compiler,
    client::{SccacheClient, make_sccache_client},
    compile_cmdline, find_compilers, write_source,
};
use pastey::paste;
use sccache::compiler::Language;
use std::{ffi::OsString, path::Path};

fn assert_stats_eq(
    expected_stats: &sccache::server::ServerStats,
    actual_stats: &sccache::server::ServerStats,
) {
    if actual_stats != expected_stats {
        panic!(
            "assertion `left == right` failed.\n  expected: {}\n    actual: {}",
            serde_json::to_string(&serde_patch::diff(actual_stats, expected_stats).unwrap())
                .unwrap(),
            serde_json::to_string(&serde_patch::diff(expected_stats, actual_stats).unwrap())
                .unwrap()
        )
    }
}

fn test_dependency_changes(
    client: &SccacheClient,
    compiler: &Compiler,
    tempdir: &Path,
    clang_plusplus: bool,
) -> sccache::errors::Result<()> {
    let Compiler {
        name,
        exe,
        env_vars,
        ..
    } = compiler;

    println!("test_dependency_changes: {name}");

    let src = "pp_test.c";
    let obj = "pp_test.c.o";
    let dep_a = "pp_test_a.h";
    let dep_b = "pp_test_b.h";

    let kind = (*name).into();
    let lang = if clang_plusplus {
        Language::Cxx
    } else {
        Language::C
    };

    client.zero_stats();
    client.clear_disk_cache()?;

    let mut expected_stats = client.stats().unwrap();

    let mut compile_and_verify =
        |additional_stats: AdditionalStats| -> sccache::errors::Result<()> {
            let mut cmd = client.cmd();
            cmd.current_dir(tempdir)
                .envs(env_vars.clone())
                .args(compile_cmdline(name, exe, src, obj, []))
                .assert()
                .success();
            // Zero-out duration stats
            let actual_stats = client.stats().unwrap() + AdditionalStats::default();
            expected_stats += additional_stats;
            assert_stats_eq(&expected_stats, &actual_stats);
            Ok(())
        };

    {
        // Write source file
        write_source(
            tempdir,
            src,
            &format!(
                r#"
#include <stdio.h>
#include "{dep_a}"
int main(int argc, char** argv) {{
    return a();
}}
"#
            ),
        );

        // Write dependency A
        write_source(tempdir, dep_a, r#"int a() { return 0; }"#);

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    // Change dep_a and recompile
    {
        write_source(
            tempdir,
            dep_a,
            &format!(
                r#"
#include "{dep_b}"
int a() {{ return b(); }}
"#
            ),
        );

        // Write dependency B
        write_source(tempdir, dep_b, r#"int b() { return 1; }"#);

        // Should be two object hashes now
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        // Should still be two object hashes
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    // Change dep_b and recompile
    {
        write_source(tempdir, dep_b, r#"int b() { return 2; }"#);

        // Should be three object hashes now
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        // Should still be three object hashes
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    // Revert dep_b to ensure preprocessor cache hit
    {
        // Write dependency B
        write_source(tempdir, dep_b, r#"int b() { return 1; }"#);

        // Should still be three object hashes
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    // Revert dep_a to ensure preprocessor cache hit
    {
        // Write dependency A
        write_source(tempdir, dep_a, r#"int a() { return 0; }"#);

        // Should still be three object hashes
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    Ok(())
}

fn test_time_macros(
    client: &SccacheClient,
    compiler: &Compiler,
    tempdir: &Path,
    clang_plusplus: bool,
) -> sccache::errors::Result<()> {
    let Compiler {
        name,
        exe,
        env_vars,
        ..
    } = compiler;

    println!("test_time_macros: {name}");

    let src = "time_macros_test.c";
    let obj = "time_macros_test.c.o";
    let dep_a = "time_macros_test_a.h";
    let dep_b = "time_macros_test_b.h";

    let kind = (*name).into();
    let lang = if clang_plusplus {
        Language::Cxx
    } else {
        Language::C
    };

    client.zero_stats();
    client.clear_disk_cache()?;

    let mut expected_stats = client.stats().unwrap();

    let mut compile_and_verify =
        |additional_stats: AdditionalStats| -> sccache::errors::Result<()> {
            let mut cmd = client.cmd();
            cmd.current_dir(tempdir)
                .envs(env_vars.clone())
                .args(compile_cmdline(name, exe, src, obj, []))
                .assert()
                .success();
            // Zero-out duration stats
            let actual_stats = client.stats().unwrap() + AdditionalStats::default();
            expected_stats += additional_stats;
            assert_stats_eq(&expected_stats, &actual_stats);
            Ok(())
        };

    // Test __TIME__ macro used in main source file
    {
        // Write source file with __TIME__ macro
        write_source(
            tempdir,
            src,
            r#"
#include <stdio.h>
int main(int argc, char** argv) {{
    printf("time: %s", __TIME__);
    return 0;
}}
    "#,
        );

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            non_cacheable_compilations: Some(1),
            ..Default::default()
        })?;

        // Wait 2s and compile again to ensure preprocessor cache miss
        std::thread::sleep(std::time::Duration::from_secs(2));

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            non_cacheable_compilations: Some(1),
            ..Default::default()
        })?;
    }

    // Test time macro used in dependency
    {
        write_source(
            tempdir,
            src,
            &format!(
                r#"
#include "{dep_a}"
int main(int argc, char** argv) {{
    a();
    return 0;
}}
"#
            ),
        );

        // Write dependency A with __TIME__ macro
        write_source(
            tempdir,
            dep_a,
            r#"
#include <stdio.h>
void a() { printf("time: %s", __TIME__); }
"#,
        );

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            non_cacheable_compilations: Some(1),
            ..Default::default()
        })?;

        // Wait 2s and compile again to ensure preprocessor cache miss
        std::thread::sleep(std::time::Duration::from_secs(2));

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            non_cacheable_compilations: Some(1),
            ..Default::default()
        })?;
    }

    // Test time macro used in dependency of a dependency
    {
        write_source(
            tempdir,
            dep_a,
            &format!(
                r#"
#include "{dep_b}"
void a() {{ b(); }}
"#
            ),
        );

        // Write dependency B with __TIME__ macro
        write_source(
            tempdir,
            dep_b,
            r#"
#include <stdio.h>
void b() { printf("time: %s", __TIME__); }
"#,
        );

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            non_cacheable_compilations: Some(1),
            ..Default::default()
        })?;

        // Wait 2s and compile again to ensure preprocessor cache miss
        std::thread::sleep(std::time::Duration::from_secs(2));

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            non_cacheable_compilations: Some(1),
            ..Default::default()
        })?;
    }

    // Test removing __TIME__ macro from dependency B
    {
        // Remove __TIME__ macro from dependency B
        write_source(tempdir, dep_b, r#"void b() {}"#);

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            // non_cacheable_compilations: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Wait 2s and compile again to ensure preprocessor cache hit
        std::thread::sleep(std::time::Duration::from_secs(2));

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    Ok(())
}

fn test_date_macros(
    client: &SccacheClient,
    compiler: &Compiler,
    tempdir: &Path,
    clang_plusplus: bool,
) -> sccache::errors::Result<()> {
    if (compiler.name == "clang" || compiler.name == "clang++")
        && !compiler
            .version()
            .and_then(|clang_v| {
                version_compare::compare_to(clang_v, "16.0.0", version_compare::Cmp::Ge)
                    .map_err(|_| sccache::errors::anyhow!("clang too old"))
            })
            .unwrap_or_default()
    {
        return Ok(());
    }

    let Compiler {
        name,
        exe,
        env_vars,
        ..
    } = compiler;

    println!("test_date_macros: {name}");

    let src = "date_macros_test.c";
    let obj = "date_macros_test.c.o";
    let dep_a = "date_macros_test_a.h";
    let dep_b = "date_macros_test_b.h";

    let kind = (*name).into();
    let lang = if clang_plusplus {
        Language::Cxx
    } else {
        Language::C
    };

    client.zero_stats();
    client.clear_disk_cache()?;

    let mut expected_stats = client.stats().unwrap();

    let mut compile_and_verify =
        |envs: &[(OsString, OsString)], additional_stats| -> sccache::errors::Result<()> {
            let mut cmd = client.cmd();
            cmd.current_dir(tempdir)
                .envs(envs.to_vec())
                .args(compile_cmdline(name, exe, src, obj, []))
                .assert()
                .success();
            // Zero-out duration stats
            let actual_stats = client.stats().unwrap() + AdditionalStats::default();
            expected_stats += additional_stats;
            assert_stats_eq(&expected_stats, &actual_stats);
            Ok(())
        };

    let s_epoch_today = chrono::Local::now().to_utc();
    let s_epoch_tomorrow = s_epoch_today
        .checked_add_days(chrono::Days::new(1))
        .unwrap();
    let s_epoch_today = s_epoch_today.timestamp().to_string();
    let s_epoch_tomorrow = s_epoch_tomorrow.timestamp().to_string();

    // Test __DATE__ macro used in main source file
    {
        // Write source file with __DATE__ macro
        write_source(
            tempdir,
            src,
            r#"
#include <stdio.h>
int main(int argc, char** argv) {{
    printf("date: %s", __DATE__);
    return 0;
}}
    "#,
        );

        // Compile "today" and populate preprocessor and object disk caches
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" to ensure cache miss
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;
    }

    // Test date macro used in dependency
    {
        write_source(
            tempdir,
            src,
            &format!(
                r#"
#include "{dep_a}"
int main(int argc, char** argv) {{
    a();
    return 0;
}}
"#
            ),
        );

        // Write dependency A with __DATE__ macro
        write_source(
            tempdir,
            dep_a,
            r#"
#include <stdio.h>
void a() { printf("date: %s", __DATE__); }
"#,
        );

        // Compile "today" and populate preprocessor and object disk caches
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" to ensure cache miss
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;
    }

    // Test date macro used in dependency of a dependency
    {
        write_source(
            tempdir,
            dep_a,
            &format!(
                r#"
#include "{dep_b}"
void a() {{ b(); }}
"#
            ),
        );

        // Write dependency B with __DATE__ macro
        write_source(
            tempdir,
            dep_b,
            r#"
#include <stdio.h>
void b() { printf("date: %s", __DATE__); }
"#,
        );

        // Compile "today" and populate preprocessor and object disk caches
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "today" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" to ensure cache miss
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "today" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;
    }

    // Test removing __DATE__ from dependency B
    {
        // Write dependency B without __DATE__ macro
        write_source(tempdir, dep_b, r#"void b() {}"#);

        // Compile "today" and populate preprocessor and object disk caches
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compilations: Some(1),
                cache_writes: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_misses: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "today" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_today.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;

        // Compile "tomorrow" again to ensure preprocessor cache hit
        compile_and_verify(
            &[
                &env_vars[..],
                &[("SOURCE_DATE_EPOCH".into(), s_epoch_tomorrow.as_str().into())],
            ]
            .concat(),
            AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            },
        )?;
    }

    Ok(())
}

fn test_timestamp_macros(
    client: &SccacheClient,
    compiler: &Compiler,
    tempdir: &Path,
    clang_plusplus: bool,
) -> sccache::errors::Result<()> {
    use sccache::errors::Result;

    let Compiler {
        name,
        exe,
        env_vars,
        ..
    } = compiler;

    println!("test_timestamp_macros: {name}");

    let src = "timestamp_macro_test.c";
    let obj = "timestamp_macro_test.c.o";
    let dep_a = "timestamp_macro_test_a.h";
    let dep_b = "timestamp_macro_test_b.h";

    let kind = (*name).into();
    let lang = if clang_plusplus {
        Language::Cxx
    } else {
        Language::C
    };

    client.zero_stats();
    client.clear_disk_cache()?;

    let mut expected_stats = client.stats().unwrap();

    let mut compile_and_verify = |additional_stats: AdditionalStats| -> Result<()> {
        let mut cmd = client.cmd();
        cmd.current_dir(tempdir)
            .envs(env_vars.clone())
            .args(compile_cmdline(name, exe, src, obj, []))
            .assert()
            .success();
        // Zero-out duration stats
        let actual_stats = client.stats().unwrap() + AdditionalStats::default();
        expected_stats += additional_stats;
        assert_stats_eq(&expected_stats, &actual_stats);
        Ok(())
    };

    // Test __TIMESTAMP__ macro used in main source file
    {
        // Write source file with __TIMESTAMP__ macro
        write_source(
            tempdir,
            src,
            r#"
#include <stdio.h>
int main(int argc, char** argv) {{
    printf("timestamp 1: %s", __TIMESTAMP__);
    return 0;
}}
    "#,
        );

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Wait 2s, update source file with __TIMESTAMP__ macro,
        // then compile again to ensure preprocessor cache miss
        std::thread::sleep(std::time::Duration::from_secs(2));

        write_source(
            tempdir,
            src,
            r#"
#include <stdio.h>
int main(int argc, char** argv) {{
    printf("timestamp 2: %s", __TIMESTAMP__);
    return 0;
}}
    "#,
        );

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    // Test timestamp macro used in dependency
    {
        std::thread::sleep(std::time::Duration::from_secs(2));

        write_source(
            tempdir,
            src,
            &format!(
                r#"
#include "{dep_a}"
int main(int argc, char** argv) {{
    a();
    return 0;
}}
"#
            ),
        );

        // Write dependency A with __TIMESTAMP__ macro
        write_source(
            tempdir,
            dep_a,
            r#"
#include <stdio.h>
void a() { printf("timestamp: %s", __TIMESTAMP__); }
"#,
        );

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Wait 2s, update dependency A with __TIMESTAMP__ macro,
        // then compile again to ensure preprocessor cache miss
        std::thread::sleep(std::time::Duration::from_secs(2));

        write_source(
            tempdir,
            dep_a,
            r#"
#include <stdio.h>
void a() { printf("timestamp: %s", __TIMESTAMP__); }
"#,
        );

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    // Test timestamp macro used in dependency of a dependency
    {
        std::thread::sleep(std::time::Duration::from_secs(2));

        write_source(
            tempdir,
            dep_a,
            &format!(
                r#"
#include "{dep_b}"
void a() {{ b(); }}
"#
            ),
        );

        // Write dependency B with __TIMESTAMP__ macro
        write_source(
            tempdir,
            dep_b,
            r#"
#include <stdio.h>
void b() { printf("timestamp: %s", __TIMESTAMP__); }
"#,
        );

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Wait 2s, update dependency B with __TIMESTAMP__ macro,
        // then compile again to ensure preprocessor cache miss
        std::thread::sleep(std::time::Duration::from_secs(2));

        let prev_mtime = std::fs::metadata(tempdir.join(dep_b))?.modified()?;

        write_source(
            tempdir,
            dep_b,
            r#"
#include <stdio.h>
void b() { printf("timestamp: %s", __TIMESTAMP__); }
"#,
        );

        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Reset the mtime and compile again to validate preprocessor cache hits
        // Don't error if this fails due to permissions issues (i.e. in Windows CI)
        let mtime_reset = std::fs::File::open(tempdir.join(dep_b))
            .and_then(|f| f.set_modified(prev_mtime).map(|_| f))
            .and_then(|f| f.sync_all());

        if mtime_reset.is_ok() {
            // Compile again to ensure preprocessor cache hit
            compile_and_verify(AdditionalStats {
                hashed: Some(1),
                compile_requests: Some(1),
                requests_executed: Some(1),
                cache_hits: Some(vec![(kind, lang, 1)]),
                ..Default::default()
            })?;
        }
    }

    // Test removing __TIMESTAMP__ from dependency B
    {
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Write dependency B without __TIMESTAMP__ macro
        write_source(tempdir, dep_b, r#"void b() {}"#);

        // Compile and populate preprocessor and object disk caches
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compilations: Some(1),
            cache_writes: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_misses: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;

        // Compile again to ensure preprocessor cache hit
        compile_and_verify(AdditionalStats {
            hashed: Some(1),
            compile_requests: Some(1),
            requests_executed: Some(1),
            cache_hits: Some(vec![(kind, lang, 1)]),
            ..Default::default()
        })?;
    }

    Ok(())
}

macro_rules! sccache_macro_tests {
    ($compiler:ident, $compiler_name:expr) => {
        paste! {
            #[test]
            #[cfg(any(unix, target_env = "msvc"))]
            fn [<test_sccache_dependency_changes_ $compiler>] () {
                let _ = env_logger::try_init();
                let compilers = find_compilers();
                if let Some(compiler) = compilers.iter().find(|c| c.name == $compiler_name) {
                    // Create and start the sccache client
                    let (_tempdir, tempdir_path, client) = make_sccache_client();
                    test_dependency_changes(
                        &client.start(),
                        &compiler,
                        &tempdir_path,
                        $compiler_name == "clang++"
                    )
                    .unwrap();
                }
            }
        }

        paste! {
            #[test]
            #[cfg(any(unix, target_env = "msvc"))]
            fn [<test_sccache_macros_time_ $compiler>] () {
                let _ = env_logger::try_init();
                let compilers = find_compilers();
                if let Some(compiler) = compilers.iter().find(|c| c.name == $compiler_name) {
                    // Create and start the sccache client
                    let (_tempdir, tempdir_path, client) = make_sccache_client();
                    test_time_macros(
                        &client.start(),
                        &compiler,
                        &tempdir_path,
                        $compiler_name == "clang++"
                    )
                    .unwrap();
                }
            }
        }

        paste! {
            #[test]
            #[cfg(any(unix, target_env = "msvc"))]
            fn [<test_sccache_macros_date_ $compiler>] () {
                // MSVC and NVHPC don't respect SOURCE_DATE_EPOCH
                if !matches!($compiler_name, "cl" | "nvc" | "nvc++") {
                    let _ = env_logger::try_init();
                    let compilers = find_compilers();
                    if let Some(compiler) = compilers.iter().find(|c| c.name == $compiler_name) {
                        // Create and start the sccache client
                        let (_tempdir, tempdir_path, client) = make_sccache_client();
                        test_date_macros(
                            &client.start(),
                            &compiler,
                            &tempdir_path,
                            $compiler_name == "clang++"
                        )
                        .unwrap();
                    }
                }
            }
        }

        paste! {
            #[test]
            #[cfg(any(unix, target_env = "msvc"))]
            fn [<test_sccache_macros_timestamp_ $compiler>] () {
                let _ = env_logger::try_init();
                let compilers = find_compilers();
                if let Some(compiler) = compilers.iter().find(|c| c.name == $compiler_name) {
                    // Create and start the sccache client
                    let (_tempdir, tempdir_path, client) = make_sccache_client();
                    test_timestamp_macros(
                        &client.start(),
                        &compiler,
                        &tempdir_path,
                        $compiler_name == "clang++"
                    )
                    .unwrap();
                }
            }
        }
    };
}

#[cfg(unix)]
sccache_macro_tests!(gcc, "gcc");
#[cfg(unix)]
sccache_macro_tests!(clang, "clang");
#[cfg(unix)]
sccache_macro_tests!(clangxx, "clang++");
#[cfg(unix)]
sccache_macro_tests!(nvc, "nvc");
#[cfg(unix)]
sccache_macro_tests!(nvcxx, "nvc++");

#[cfg(target_env = "msvc")]
sccache_macro_tests!(msvc, "cl");
