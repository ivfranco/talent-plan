use kvs::{
    cmd::{error_exit, free_arg_or, help, no_more_args, version, CmdError},
    Error as KvError, KvStore,
};
use pico_args::Arguments;

fn main() {
    if let Err(e) = app() {
        error_exit(e, env!("CARGO_BIN_NAME"));
    }
}

fn app() -> Result<(), CmdError> {
    let mut args = Arguments::from_env();

    if args.contains(["-h", "--help"]) {
        help(env!("CARGO_BIN_NAME"), HELP);
        return Ok(());
    }

    if args.contains(["-V", "--version"]) {
        version(env!("CARGO_BIN_NAME"));
        return Ok(());
    }

    let sub = match args.subcommand() {
        Ok(Some(sub)) => sub,
        Ok(None) => return Err(CmdError::new("no subcommand")),
        Err(e) => return Err(CmdError::new(e)),
    };

    run_command(&sub, args)
}

fn run_command(sub: &str, mut args: Arguments) -> Result<(), CmdError> {
    const MISSING_KEY: &str = "missing storage key";
    const MISSING_VALUE: &str = "missing storage value";

    let mut kv_store = KvStore::open(std::env::current_dir()?)?;
    match sub {
        "get" => {
            let key = free_arg_or(&mut args, MISSING_KEY)?;
            no_more_args(args)?;

            let value = kv_store.get(key)?;
            if let Some(value) = value {
                print!("{}", value);
            } else {
                print!("Key not found");
            }
        }
        "set" => {
            let key = free_arg_or(&mut args, MISSING_KEY)?;
            let value = free_arg_or(&mut args, MISSING_VALUE)?;
            no_more_args(args)?;

            kv_store.set(key, value)?;
        }
        "rm" => {
            let key = free_arg_or(&mut args, MISSING_KEY)?;
            no_more_args(args)?;

            if let Err(e @ KvError::KeyNotFound) = kv_store.remove(key) {
                print!("Key not found");
                return Err(CmdError::new(e));
            }
        }
        _ => return Err(CmdError::new("unknown subcommand")),
    }

    Ok(())
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
