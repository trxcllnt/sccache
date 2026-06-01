mod cmdline;

use cmdline::Command;
use sccache::{errors::*, mock_command::ProcessOutput, util::temppath};
use std::{env, process};

fn main() {
    let command = match cmdline::try_parse_from(env::args()) {
        Ok(cmd) => cmd,
        Err(e) => match e.downcast::<clap::error::Error>() {
            Ok(clap_err) => clap_err.exit(),
            Err(some_other_err) => {
                println!("sccache-test: {some_other_err}");
                for source_err in some_other_err.chain().skip(1) {
                    println!("sccache-test: caused by: {source_err}");
                }
                process::exit(1);
            }
        },
    };

    process::exit(match run(command) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("sccache-test: error: {e}");

            for e in e.chain().skip(1) {
                eprintln!("sccache-test: caused by: {e}");
            }
            2
        }
    });
}

fn run(command: Command) -> Result<()> {
    let mut cmd = match command {
        Command::PretendToBeGcc => {
            println!("compiler_id=gcc");
            println!(
                "compiler_version=sccache-test-{}",
                env!("CARGO_PKG_VERSION")
            );
            return Ok(());
        }
        Command::RunSccache(cmd) => match cmd.as_slice() {
            [exe, args @ ..] => {
                let mut cmd = process::Command::new("sccache");
                cmd.env("SCCACHE_DIRECT", "0")
                    .arg(env::current_exe()?)
                    // Test command
                    .arg("--param")
                    .arg(
                        [exe]
                            .into_iter()
                            .chain(args)
                            .map(|s| format!("{s:?}"))
                            .collect::<Vec<String>>()
                            .join(" "),
                    )
                    // Dummy language
                    .args(["-x", "c"])
                    // Dummy input
                    .arg("-c")
                    .arg(temppath()?)
                    // Dummy output
                    .arg("-o")
                    .arg(temppath()?);
                cmd
            }
            _ => unreachable!(""),
        },
        Command::HashInputs(cmd) => match cmd.as_slice() {
            [exe, args @ ..] => {
                eprintln!("HashInputs exe: {exe:?}, args: {args:?}");
                println!(
                    "{exe:?} {:?}",
                    args.iter()
                        .map(|s| format!("{s:?}"))
                        .collect::<Vec<String>>()
                        .join(" ")
                );
                return Ok(());
            }
            _ => unreachable!(""),
        },
        Command::InvokeTest(cmd, out) => match cmd.as_slice() {
            [exe, args @ ..] => {
                eprintln!("InvokeTest exe: {exe:?}, args: {args:?}, out: {out:?}");
                let _ = std::fs::File::create(out)?;
                let mut cmd = process::Command::new(exe);
                cmd.args(args);
                cmd
            }
            _ => unreachable!(""),
        },
    };

    cmd.stdout(process::Stdio::inherit())
        .stderr(process::Stdio::inherit())
        .output()
        .map_err(|err| err.into())
        .and_then(|out| ProcessOutput::from(out).into())
        .map(|_| ())
}
