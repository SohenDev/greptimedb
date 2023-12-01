use std::sync::Arc;
use std::{fs, path};

use catalog::kvbackend::KvBackendCatalogManager;
use catalog::CatalogManagerRef;
use clap::Parser;
use common_base::Plugins;
use common_config::{metadata_store_dir, KvBackendConfig, WalConfig};
use common_meta::cache_invalidator::DummyKvCacheInvalidator;
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::LoggingOptions;
use datanode::config::{DatanodeOptions, RegionEngineConfig, StorageConfig};
use datanode::datanode::{DatanodeBuilder, ProcedureConfig};
use datanode::region_server::RegionServer;
use file_engine::config::EngineConfig as FileEngineConfig;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance as FeInstance, StandaloneDatanodeManager};
use frontend::service_config::{
    GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PromStoreOptions,
};
use mito2::config::MitoConfig;
use serde::{Deserialize, Serialize};
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use servers::Mode;
use snafu::ResultExt;
use tracing::info;

use crate::error::{
    CreateDirSnafu, IllegalConfigSnafu, InitMetadataSnafu, Result, StartDatanodeSnafu,
    StartFrontendSnafu, StartProcedureManagerSnafu,
};
use crate::options::{MixOptions, Options};

/// Build frontend instance in standalone mode
async fn build_frontend(
    plugins: Plugins,
    kv_store: KvBackendRef,
    procedure_manager: ProcedureManagerRef,
    catalog_manager: CatalogManagerRef,
    region_server: RegionServer,
) -> Result<FeInstance> {
    let frontend_instance = FeInstance::try_new_standalone(
        kv_store,
        procedure_manager,
        catalog_manager,
        plugins,
        region_server,
    )
    .await
    .context(StartFrontendSnafu)?;
    Ok(frontend_instance)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StandaloneOptions {
    pub mode: Mode,
    pub enable_telemetry: bool,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub prom_store: PromStoreOptions,
    pub wal: WalConfig,
    pub storage: StorageConfig,
    pub metadata_store: KvBackendConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
    pub user_provider: Option<String>,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
}

impl Default for StandaloneOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_telemetry: true,
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            prom_store: PromStoreOptions::default(),
            wal: WalConfig::default(),
            storage: StorageConfig::default(),
            metadata_store: KvBackendConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
            user_provider: None,
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
        }
    }
}

impl StandaloneOptions {
    fn frontend_options(self) -> FrontendOptions {
        FrontendOptions {
            mode: self.mode,
            http: self.http,
            grpc: self.grpc,
            mysql: self.mysql,
            postgres: self.postgres,
            opentsdb: self.opentsdb,
            influxdb: self.influxdb,
            prom_store: self.prom_store,
            meta_client: None,
            ..Default::default()
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            node_id: Some(0),
            wal: self.wal,
            storage: self.storage,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Parser)]
pub struct Standalone {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(long)]
    opentsdb_addr: Option<String>,
    #[clap(long)]
    influxdb_enable: bool,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(short = 'm', long = "memory-catalog")]
    enable_memory_catalog: bool,
    #[clap(long)]
    tls_mode: Option<TlsMode>,
    #[clap(long)]
    tls_cert_path: Option<String>,
    #[clap(long)]
    tls_key_path: Option<String>,
    #[clap(long)]
    user_provider: Option<String>,
}

impl Standalone {
    pub fn load_options(&self) -> Result<Options> {
        let mut opts: StandaloneOptions =
            Options::load_layered_options(self.config_file.as_deref(), "ENGRAM_", None)?;

        opts.mode = Mode::Standalone;

        let tls_opts = TlsOption::new(
            self.tls_mode.clone(),
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
        );

        if let Some(addr) = &self.http_addr {
            opts.http.addr = addr.clone();
        }

        if let Some(addr) = &self.rpc_addr {
            // frontend grpc addr conflict with datanode default grpc addr
            let datanode_grpc_addr = DatanodeOptions::default().rpc_addr;
            if addr.eq(&datanode_grpc_addr) {
                return IllegalConfigSnafu {
                    msg: format!(
                        "gRPC listen address conflicts with datanode reserved gRPC addr: {datanode_grpc_addr}",
                    ),
                }
                .fail();
            }
            opts.grpc.addr = addr.clone();
        }

        if let Some(addr) = &self.mysql_addr {
            let mysql_opts = &mut opts.mysql;
            mysql_opts.addr = addr.clone();
            mysql_opts.tls = tls_opts.clone();
        }

        if let Some(addr) = &self.postgres_addr {
            let postgres_opts = &mut opts.postgres;
            postgres_opts.addr = addr.clone();
            postgres_opts.tls = tls_opts;
        }

        if let Some(addr) = &self.opentsdb_addr {
            let opentsdb_addr = &mut opts.opentsdb;
            opentsdb_addr.addr = addr.clone();
        }

        if self.influxdb_enable {
            opts.influxdb = InfluxdbOptions { enable: true };
        }

        let metadata_store = opts.metadata_store.clone();
        let procedure = opts.procedure.clone();
        let frontend = opts.clone().frontend_options();
        let logging = opts.logging.clone();
        let datanode = opts.datanode_options();

        Ok(Options::Standalone(Box::new(MixOptions {
            procedure,
            metadata_store,
            data_home: datanode.storage.data_home.to_string(),
            frontend,
            datanode,
            logging,
        })))
    }

    pub async fn execute(self, opts: MixOptions) -> Result<()> {
        let mut fe_opts = opts.frontend.clone();
        let dn_opts = opts.datanode.clone();
        let fe_plugins = plugins::setup_frontend_plugins(&mut fe_opts) // mut ref is MUST, DO NOT change it
            .await
            .context(StartFrontendSnafu)?;

        info!("Standalone start command: {:#?}", self);
        info!(
            "Standalone frontend options: {:#?}, datanode options: {:#?}",
            fe_opts, dn_opts
        );

        // Ensure the data_home directory exists.
        fs::create_dir_all(path::Path::new(&opts.data_home)).context(CreateDirSnafu {
            dir: &opts.data_home,
        })?;

        let metadata_dir = metadata_store_dir(&opts.data_home);
        let (kv_backend, procedure_manager) = FeInstance::try_build_standalone_components(
            metadata_dir,
            opts.metadata_store.clone(),
            opts.procedure.clone(),
        )
        .await
        .context(StartFrontendSnafu)?;

        let mut datanode = DatanodeBuilder::new(
            dn_opts.clone(),
            Some(kv_backend.clone()),
            Default::default(),
        )
        .build()
        .await
        .context(StartDatanodeSnafu)?;
        let region_server = datanode.region_server();

        let catalog_manager = KvBackendCatalogManager::new(
            kv_backend.clone(),
            Arc::new(DummyKvCacheInvalidator),
            Arc::new(StandaloneDatanodeManager(region_server.clone())),
        );

        catalog_manager
            .table_metadata_manager_ref()
            .init()
            .await
            .context(InitMetadataSnafu)?;
        info!("Datanode instance started");

        let mut frontend = build_frontend(
            fe_plugins,
            kv_backend,
            procedure_manager.clone(),
            catalog_manager,
            region_server,
        )
        .await?;

        frontend
            .build_servers(opts)
            .await
            .context(StartFrontendSnafu)?;

        datanode.start().await.context(StartDatanodeSnafu)?;
        procedure_manager
            .start()
            .await
            .context(StartProcedureManagerSnafu)?;
        frontend.start().await.context(StartFrontendSnafu)?;

        Ok(())
    }
}
