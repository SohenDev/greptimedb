use std::fmt::Display;

use clap::{Parser, Subcommand};
use cmd::error::Error;
use cmd::options::Options;
use cmd::panic_hook::set_panic_hook;
use cmd::subcmd::{repl, standalone};
use futures::executor::block_on;
use tracing::info;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, Registry};

#[derive(Parser)]
#[clap(name = "Engram")]
#[clap(author, version, about, long_about = None)] // Read from `Cargo.toml`
struct Engram {
    pub dir: Option<String>,
    pub level: Option<String>,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Standalone(standalone::Standalone),
    REPL(repl::REPL),
}

impl Commands {
    pub fn execute(self) -> Result<(), Error> {
        let opts = match &self {
            Commands::Standalone(cmd) => cmd.load_options(),
            Commands::REPL(cmd) => cmd.load_options(),
        }?;
        match (self, opts) {
            (Commands::Standalone(cmd), Options::Standalone(opts)) => block_on(cmd.execute(*opts)),
            (Commands::REPL(cmd), Options::Cli(_)) => block_on(cmd.execute()),
            _ => unreachable!(),
        }
    }
}

impl Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Commands::Standalone(_) => f.write_str("standalone"),
            Commands::REPL(_) => f.write_str("repl"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli: Engram = Engram::parse();
    set_panic_hook();

    let filter = cli
        .level
        .unwrap_or("info".to_string())
        .parse::<filter::Targets>()
        .expect("error parsing log level string");

    let (stdout_writer, _stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_logging_layer = Layer::new().with_writer(stdout_writer);

    let subscriber = Registry::default().with(filter).with(stdout_logging_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");

    info!("starting engram command {}", cli.command);
    return cli.command.execute();
}
