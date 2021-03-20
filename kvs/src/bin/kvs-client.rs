use std::net::SocketAddr;

use kvs::{client::KvsClient, cmd::*};
use pico_args::Arguments;

#[tokio::main]
async fn main() {
    let args = Arguments::from_env();
    if let Err(e) = cmd(args).await {
        error_exit(e, env!("CARGO_BIN_NAME"));
    }
}

async fn cmd(mut args: Arguments) -> Result<(), CmdError> {
    if args.contains(["-h", "--help"]) {
        help(env!("CARGO_BIN_NAME"), HELP);
        return Ok(());
    }

    if args.contains(["-V", "--version"]) {
        version(env!("CARGO_BIN_NAME"));
        return Ok(());
    }

    let sub = args
        .subcommand()?
        .ok_or_else(|| CmdError::new("no subcommand"))?;

    let addr: SocketAddr = args
        .opt_value_from_str("--addr")?
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 4000)));

    run_command(&sub, addr, args).await?;

    Ok(())
}

async fn run_command(sub: &str, addr: SocketAddr, mut args: Arguments) -> Result<(), CmdError> {
    const MISSING_KEY: &str = "missing storage key";
    const MISSING_VALUE: &str = "missing storage value";

    let mut client = KvsClient::connect(addr).await?;
    match sub {
        "get" => {
            let key = free_arg_or(&mut args, MISSING_KEY)?;
            no_more_args(args)?;

            let value = client.get(key).await?;
            if let Some(value) = value {
                println!("{}", value);
            } else {
                print!("Key not found");
            }
        }
        "set" => {
            let key = free_arg_or(&mut args, MISSING_KEY)?;
            let value = free_arg_or(&mut args, MISSING_VALUE)?;
            no_more_args(args)?;

            client.set(key, value).await?;
        }
        "rm" => {
            let key = free_arg_or(&mut args, MISSING_KEY)?;
            no_more_args(args)?;

            client.remove(key).await?;
        }
        _ => return Err(CmdError::new("unknown subcommand")),
    }

    Ok(())
}

const HELP: &str = "\
Send commands to a remove key-value store server.

USAGE:
    kvs-client [FLAGS] [SUBCOMMAND]

FLAGS:
    -h, --help          Prints this message 
    -V, --version       Prints version information

SUBCOMMANDS:
    set <KEY> <VALUE> [--addr IP-PORT]  Set the value of a string key to a string.
    get <KEY> [--addr IP-PORT]          Get the string value of a given string key.
    rm  <KEY> [--addr IP-PORT]          Remove a given string key.

IP-PORT:
    <IP:PORT>           Start the server with the specified IP (v4 or v6) and port.
                        Default to 127.0.0.1:4000.
";
