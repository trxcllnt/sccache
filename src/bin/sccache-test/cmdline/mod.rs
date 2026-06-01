use std::{ffi::OsString, path::PathBuf};

mod parse;

pub use parse::try_parse_from;

#[derive(Debug)]
pub enum Command {
    InvokeTest(Vec<OsString>, PathBuf),
    HashInputs(Vec<OsString>),
    PretendToBeGcc,
    RunSccache(Vec<OsString>),
}
