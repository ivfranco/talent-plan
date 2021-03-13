use kvs::{KvStore, Result};
use pico_args::Arguments;
use std::{fmt::Display, process};

fn main() {
    let mut args = Arguments::from_env();

    if args.contains(["-h", "--help"]) {
        help();
        return;
    }

    if args.contains(["-V", "--version"]) {
        version();
        return;
    }

    let sub = match args.subcommand() {
        Ok(Some(sub)) => sub,
        Ok(None) => error_exit("no subcommand"),
        Err(e) => error_exit(e),
    };

    if let Err(err) = run_command(&sub, &mut args) {
        error_exit(err);
    };
}

fn run_command(sub: &str, args: &mut Arguments) -> Result<()> {
    const MISSING_KEY: &str = "missing storage key";
    const MISSING_VALUE: &str = "missing storage value";

    let mut kv_store = KvStore::open(std::env::current_dir()?)?;
    match sub {
        "get" => {
            let key = free_arg_or(args, MISSING_KEY);
            no_more_args(args);

            let value = kv_store.get(key)?;
            if let Some(value) = value {
                print!("{}", value);
            } else {
                print!("Key not found");
            }
        }
        "set" => {
            let key = free_arg_or(args, MISSING_KEY);
            let value = free_arg_or(args, MISSING_VALUE);
            no_more_args(args);

            kv_store.set(key, value)?;
        }
        "rm" => {
            let key = free_arg_or(args, MISSING_KEY);
            no_more_args(args);

            kv_store.remove(key)?;
        }
        _ => error_exit("unknown subcommand"),
    }

    Ok(())
}

fn error_exit<T: Display>(err: T) -> ! {
    print!("{}", err);
    eprintln!(
        "Try '{} --help' for more information.",
        env!("CARGO_PKG_NAME")
    );
    process::exit(1)
}

fn no_more_args(args: &mut Arguments) {
    if free_arg(args).is_some() {
        error_exit("extra arguments");
    }
}

fn free_arg(args: &mut Arguments) -> Option<String> {
    match args.free_from_str() {
        Ok(arg) => Some(arg),
        Err(pico_args::Error::MissingArgument) => None,
        Err(e) => error_exit(e),
    }
}

fn free_arg_or<T: Display>(args: &mut Arguments, err: T) -> String {
    free_arg(args).unwrap_or_else(|| error_exit(err))
}

fn version() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
}

fn help() {
    version();
    println!("{}", env!("CARGO_PKG_AUTHORS"));
    println!("{}", env!("CARGO_PKG_DESCRIPTION"));
    print!("{}", HELP);
}

const HELP: &str = "\
A command-line key-value store program.

USAGE:
    kvs [FLAGS] [SUBCOMMAND]

FLAGS:
    -h, --help          Prints this message 
    -V, --version       Prints version information

SUBCOMMAND:
    set <KEY> <VALUE>   Set the value of a string key to a string
    get <KEY>           Get the string value of a given string key
    rm  <KEY>           Remove a given key
";
