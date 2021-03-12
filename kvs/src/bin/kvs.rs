use pico_args::Arguments;
use std::{fmt::Display, process};

fn main() {
    let mut args = Arguments::from_env();

    if args.contains(["-h", "--help"]) {
        help();
        return;
    }

    if args.contains("-V") {
        version();
        return;
    }

    let sub = match args.subcommand() {
        Ok(Some(sub)) => sub,
        Ok(None) => error_exit("no subcommand"),
        Err(e) => error_exit(e),
    };

    const MISSING_KEY: &str = "missing storage key";
    const MISSING_VALUE: &str = "missing storage value";

    match sub.as_str() {
        "get" => {
            let key = free_arg(&mut args, MISSING_KEY);
            eprintln!("unimplemented");
            process::exit(1);
        }
        "set" => {
            let key = free_arg(&mut args, MISSING_KEY);
            let value = free_arg(&mut args, MISSING_VALUE);
            eprintln!("unimplemented");
            process::exit(1);
        }
        "rm" => {
            let key = free_arg(&mut args, MISSING_KEY);
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => error_exit("unknown subcommand"),
    }
}

fn free_arg<T: Display>(args: &mut Arguments, err: T) -> String {
    if let Ok(arg) = args.free_from_str() {
        arg
    } else {
        error_exit(err);
    }
}

fn error_exit<T: Display>(err: T) -> ! {
    eprintln!("{}: {}", env!("CARGO_PKG_NAME"), err);
    eprintln!(
        "Try '{} --help' for more information.",
        env!("CARGO_PKG_NAME")
    );
    process::exit(1)
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
    -V              Prints version information
    -h, --help      Prints this message 

SUBCOMMAND:
    set <KEY> <VALUE>   Set the value of a string key to a string
    get <KEY>           Get the string value of a given string key
    rm  <KEY>           Remove a given key
";
