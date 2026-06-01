use clap::{ArgAction, ArgMatches, arg};
use std::{ffi::OsString, path::PathBuf};

use sccache::{dist::strings_to_osstrings, errors::*, util::split_quoted_shell_str};

use crate::cmdline::Command;

fn get_clap_command() -> clap::Command {
    clap::Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .args(&[
            arg!(lineno: -P).action(ArgAction::SetTrue),
            arg!(lang: -x <LANG>),
            // Preprocess the test command to compute a hash
            arg!(preprocess: -E --preprocess).action(ArgAction::SetTrue),
            // Invoke the test command in `--param "${CMD@Q}"`
            arg!(compile: -c --compile).action(ArgAction::SetTrue),
            arg!(output: -o --output <FILE>)
                .action(ArgAction::Set)
                .value_parser(clap::value_parser!(PathBuf)),
            // Command to invoke when sccache-test is run with -c
            arg!(CMD: -p --param <CMD>)
                .num_args(1)
                .action(ArgAction::Set)
                .value_parser(clap::value_parser!(OsString)),
            // Run `sccache sccache-test --param "${INPUT@Q} ${ARGS@Q}" -c <input> -o <output>`
            arg!([INPUT]).value_parser(clap::value_parser!(OsString)),
            arg!([ARGS] ... "test command to run")
                .trailing_var_arg(true)
                .value_parser(clap::value_parser!(OsString)),
        ])
}

fn get_test_cmd(matches: ArgMatches) -> Vec<OsString> {
    matches
        .get_one::<OsString>("CMD")
        .and_then(|cmd| cmd.to_str())
        .inspect(|cmd| eprintln!("test command: {cmd:?}"))
        .and_then(split_quoted_shell_str)
        .map(|xs| strings_to_osstrings(&xs))
        .expect("--param must be specified")
}

pub fn try_parse_from(
    args: impl IntoIterator<Item = impl Into<OsString> + Clone>,
) -> Result<Command> {
    let matches = get_clap_command().try_get_matches_from(args)?;

    if matches.get_flag("preprocess") {
        if matches
            .get_one::<OsString>("INPUT")
            .map(PathBuf::from)
            .map(|path| {
                path.file_name()
                    .filter(|&name| name == "testfile.c")
                    .is_some()
            })
            .unwrap_or_default()
        {
            return Ok(Command::PretendToBeGcc);
        }
        return Ok(Command::HashInputs(get_test_cmd(matches)));
    }

    if matches.get_flag("compile")
        && let Some(output) = matches.get_one::<PathBuf>("output").cloned()
    {
        return Ok(Command::InvokeTest(get_test_cmd(matches), output));
    }

    if matches.contains_id("ARGS")
        && let Some(exe) = matches.get_one::<OsString>("INPUT")
        && let Some(cmd) = matches.get_many("ARGS")
    {
        return Ok(Command::RunSccache(
            [exe].into_iter().chain(cmd).cloned().collect(),
        ));
    }

    Err(anyhow!("Failed to parse arguments"))
}
