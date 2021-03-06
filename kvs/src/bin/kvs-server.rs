use kvs::{
    cmd::*,
    log_engine::LogKvsEngine,
    server::{choose_flavor, default_addr, Flavor, KvsServer},
    sled_engine::SledKvsEngine,
};
use log::{info, LevelFilter};
use pico_args::Arguments;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

    info!("{} {}", env!("CARGO_BIN_NAME"), env!("CARGO_PKG_VERSION"));

    let args = Arguments::from_env();
    if let Err(err) = cmd(args).await {
        error_exit(err, env!("CARGO_BIN_NAME"));
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

    let addr: SocketAddr = args
        .opt_value_from_str("--addr")?
        .unwrap_or_else(default_addr);
    let flavor: Option<Flavor> = args.opt_value_from_str("--engine")?;

    no_more_args(args)?;

    run_with_flavor(choose_flavor(flavor)?, addr).await?;

    Ok(())
}

async fn run_with_flavor(flavor: Flavor, addr: SocketAddr) -> Result<(), CmdError> {
    let cwd = std::env::current_dir()?;

    match flavor {
        Flavor::Kvs => {
            let engine = LogKvsEngine::open(&cwd)?;
            let (server, _switch) = KvsServer::bind(engine, addr).await?;
            server.listen().await?;
        }
        Flavor::Sled => {
            let engine = SledKvsEngine::open(&cwd)?;
            let (server, _switch) = KvsServer::bind(engine, addr).await?;
            server.listen().await?;
        }
    }

    Ok(())
}

const HELP: &str = "\
Start a server accepting and executing commands to a key-value store.

USAGE:
    kvs-server [FLAGS] [--addr IP-PORT] [--engine ENGINE-NAME]

FLAGS:
    -h, --help          Prints this message 
    -V, --version       Prints version information

IP-PORT:
    <IP:PORT>           Start the server with the specified IP (v4 or v6) and port.
                        Default to 127.0.0.1:4000.

ENGINE-NAME:
                        Default to `kvs` or the engine already in use if there is previous persisted data.
    kvs                 The build-in kvs engine
    sled                The sled embedded database
";
