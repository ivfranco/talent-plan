use std::net::SocketAddr;

use kvs::{
    cmd::*,
    server::{choose_flavor, Flavor, KvsServer},
    sled_engine::SledKvsEngine,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    LogKvsEngine,
};
use log::{info, LevelFilter};
use pico_args::Arguments;

fn main() {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

    info!("{} {}", env!("CARGO_BIN_NAME"), env!("CARGO_PKG_VERSION"));

    let args = Arguments::from_env();
    if let Err(err) = cmd(args) {
        error_exit(err, env!("CARGO_BIN_NAME"));
    }
}

fn cmd(mut args: Arguments) -> Result<(), CmdError> {
    if args.contains(["-h", "--help"]) {
        help(env!("CARGO_BIN_NAME"), HELP);
        return Ok(());
    }

    if args.contains(["-V", "--version"]) {
        version(env!("CARGO_BIN_NAME"));
        return Ok(());
    }

    let addr: Option<SocketAddr> = args.opt_value_from_str("--addr")?;
    let flavor: Option<Flavor> = args.opt_value_from_str("--engine")?;

    no_more_args(args)?;

    run_with_flavor(choose_flavor(flavor)?, addr)?;

    Ok(())
}

fn run_with_flavor(flavor: Flavor, addr: Option<SocketAddr>) -> Result<(), CmdError> {
    let cwd = std::env::current_dir()?;
    let pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;

    match flavor {
        Flavor::Kvs => {
            let engine = LogKvsEngine::open(&cwd)?;
            KvsServer::open(engine, pool).listen_on_current(addr)?;
        }
        Flavor::Sled => {
            let engine = SledKvsEngine::open(&cwd)?;
            KvsServer::open(engine, pool).listen_on_current(addr)?;
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
