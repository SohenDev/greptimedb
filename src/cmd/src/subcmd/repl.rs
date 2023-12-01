use clap::Parser;
use common_telemetry::logging::LoggingOptions;
use futures::executor::block_on;

use crate::cli::{AttachCommand, Repl};
use crate::error::Result;
use crate::options::Options;

#[derive(Clone, Debug, Parser)]
pub struct REPL {
    #[clap(short, long)]
    pub grpc_addr: String,
    #[clap(short, long)]
    pub meta_addr: Option<String>,
    #[clap(short, long)]
    pub disable_helper: bool,
}

impl REPL {
    pub async fn execute(self) -> Result<()> {
        let cmd = AttachCommand {
            grpc_addr: self.grpc_addr,
            meta_addr: self.meta_addr,
            disable_helper: self.disable_helper,
        };
        let mut repl = block_on(Repl::try_new(&cmd))?;
        repl.run().await
    }

    pub fn load_options(&self) -> Result<Options> {
        let logging_opts = LoggingOptions::default();

        Ok(Options::Cli(Box::new(logging_opts)))
    }
}
