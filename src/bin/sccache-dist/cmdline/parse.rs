// Copyright 2022 <LovecraftianHorror@pm.me>
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

use std::{env, ffi::OsString, path::PathBuf, str::FromStr};

use anyhow::{bail, Context};
use clap::{Arg, Command as ClapCommand, ValueEnum};
use sccache::config;
use syslog::Facility;

use crate::cmdline::Command;

#[derive(Clone, Copy, ValueEnum)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl FromStr for LogLevel {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let variant = match s {
            "error" => Self::Error,
            "warn" => Self::Warn,
            "info" => Self::Info,
            "debug" => Self::Debug,
            "trace" => Self::Trace,
            _ => bail!("Unknown log level: {:?}", s),
        };

        Ok(variant)
    }
}

impl From<LogLevel> for log::LevelFilter {
    fn from(log_level: LogLevel) -> Self {
        match log_level {
            LogLevel::Error => Self::Error,
            LogLevel::Warn => Self::Warn,
            LogLevel::Info => Self::Info,
            LogLevel::Debug => Self::Debug,
            LogLevel::Trace => Self::Trace,
        }
    }
}

fn flag_infer_long(name: &'static str) -> Arg {
    Arg::new(name).long(name)
}

fn get_clap_command() -> ClapCommand {
    let syslog = flag_infer_long("syslog")
        .help("Log to the syslog with LEVEL")
        .value_name("LEVEL")
        .value_parser(clap::value_parser!(LogLevel));
    let config_with_help_message = |help: &'static str| {
        flag_infer_long("config")
            .help(help)
            .value_name("PATH")
            .value_parser(clap::value_parser!(PathBuf))
    };

    ClapCommand::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .subcommand(ClapCommand::new("scheduler").args(&[
            config_with_help_message("Use the scheduler config file at PATH").required(true),
            syslog.clone(),
        ]))
        .subcommand(ClapCommand::new("server").args(&[
            config_with_help_message("Use the server config file at PATH").required(true),
            syslog,
        ]))
}

fn check_init_syslog(name: &str, log_level: LogLevel) {
    let level = log::LevelFilter::from(log_level);
    drop(syslog::init(Facility::LOG_DAEMON, level, Some(name)));
}

/// Parse commandline `args` into a `Result<Command>` to execute.
pub fn try_parse_from(
    args: impl IntoIterator<Item = impl Into<OsString> + Clone>,
) -> anyhow::Result<Command> {
    let matches = get_clap_command().try_get_matches_from(args)?;

    Ok(match matches.subcommand() {
        Some(("scheduler", matches)) => {
            if matches.contains_id("syslog") {
                let log_level = matches
                    .get_one::<LogLevel>("syslog")
                    .expect("`syslog` is required");
                check_init_syslog("sccache-scheduler", *log_level);
            }

            let config_path = matches
                .get_one::<PathBuf>("config")
                .expect("`config` is required");

            Command::Scheduler(
                config::scheduler::Config::load(Some(config_path.clone()))
                    .with_context(|| "Could not load config")?,
            )
        }
        Some(("server", matches)) => {
            if matches.contains_id("syslog") {
                let log_level = matches
                    .get_one::<LogLevel>("syslog")
                    .expect("`syslog` is required");
                check_init_syslog("sccache-buildserver", *log_level);
            }

            let config_path = matches
                .get_one::<PathBuf>("config")
                .expect("`config` is required");

            Command::Server(
                config::server::Config::load(Some(config_path.clone()))
                    .with_context(|| "Could not load config")?,
            )
        }
        _ => unreachable!("Subcommand is enforced by clap"),
    })
}
