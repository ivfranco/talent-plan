use pico_args::Arguments;
use std::{error::Error, fmt::Display, process};

/// Command line error type, unlike Box<dyn Error> may contain bare &'static str.
pub struct CmdError {
    error: Box<dyn Display + 'static>,
}

impl Display for CmdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<E: Error + 'static> From<E> for CmdError {
    fn from(e: E) -> Self {
        Self { error: Box::new(e) }
    }
}

impl CmdError {
    /// Construct a new CmdError from any Display type with 'static lifetime.
    /// # Examples:
    /// ```rust
    /// # use kvs::cmd::CmdError;
    /// CmdError::new("test error");
    /// ```
    pub fn new<E: Display + 'static>(e: E) -> Self {
        Self { error: Box::new(e) }
    }
}

/// print an error to stdout, help messages to stderr then exit with non-zero
/// exit code.
pub fn error_exit<T: Display>(err: T, bin_name: &str) -> ! {
    eprintln!("{}: {}", bin_name, err);
    eprintln!("Try '{} --help' for more information.", bin_name,);
    process::exit(1)
}

/// Return the next free argument, or None if there's no more.
fn free_arg(args: &mut Arguments) -> Result<Option<String>, CmdError> {
    let arg = match args.free_from_str() {
        Ok(arg) => Some(arg),
        Err(pico_args::Error::MissingArgument) => None,
        Err(e) => return Err(e.into()),
    };

    Ok(arg)
}

/// Return an error when there is any more free argument.
pub fn no_more_args(args: Arguments) -> Result<(), CmdError> {
    if !args.finish().is_empty() {
        Err(CmdError::new("extra arguments"))
    } else {
        Ok(())
    }
}

/// Return the next free argument, or return the given error if there's no more.
pub fn free_arg_or<T: Display + 'static>(args: &mut Arguments, err: T) -> Result<String, CmdError> {
    free_arg(args).and_then(|arg| arg.ok_or_else(|| CmdError::new(err)))
}

/// Print the name and version of the application.
pub fn version(bin_name: &str) {
    println!("{} {}", bin_name, env!("CARGO_PKG_VERSION"));
}

/// Print help name, version, author, description and help messages of the application.
pub fn help(bin_name: &str, help_msg: &str) {
    version(bin_name);
    println!("{}", env!("CARGO_PKG_AUTHORS"));
    println!("{}", env!("CARGO_PKG_DESCRIPTION"));
    print!("{}", help_msg);
}
