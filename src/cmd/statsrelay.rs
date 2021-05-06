extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use anyhow::Context;
use futures::StreamExt;
use futures::{stream::FuturesUnordered, FutureExt};
use stream_cancel::Tripwire;
use structopt::StructOpt;

use std::collections::HashMap;
use std::collections::HashSet;

use tokio::runtime;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};

use env_logger::Env;
use log::{debug, error, info};

use statsrelay::config;
use statsrelay::discovery;
use statsrelay::processors;
use statsrelay::stats;
use statsrelay::statsd_server;
use statsrelay::{admin, config::Config};
use statsrelay::{backends, stats::Scope};

#[derive(StructOpt, Debug)]
struct Options {
    #[structopt(short = "c", long = "--config", default_value = "/etc/statsrelay.json")]
    pub config: String,

    #[structopt(long = "--config-check-and-exit")]
    pub config_check: bool,

    #[structopt(short = "t", long = "--threaded")]
    pub threaded: bool,
}

/// The main server invocation, for a given configuration, options and stats
/// scope. The server will spawn any listeners, initialize a backend
/// configuration update loop, as well as register signal handlers.
async fn server(scope: stats::Scope, config: Config, opts: Options) {
    let backend_reloads = scope.counter("backend_reloads").unwrap();
    let backends = backends::Backends::new(scope.scope("backends"));

    // Load processors
    if let Some(processors) = config.processors.as_ref() {
        load_processors(scope.scope("processors"), &backends, processors)
            .await
            .unwrap();
    }

    let (sender, tripwire) = Tripwire::new();
    let mut run: FuturesUnordered<_> = config
        .statsd
        .servers
        .iter()
        .map({
            |(server_name, server_config)| {
                let name = server_name.clone();
                statsd_server::run(
                    scope.scope("statsd_server").scope(server_name),
                    tripwire.clone(),
                    server_config.clone(),
                    backends.clone(),
                )
                .map(|_| name)
            }
        })
        .collect();

    // Trap ctrl+c and sigterm messages and perform a clean shutdown
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::spawn(async move {
        select! {
        _ = sigint.recv() => info!("received sigint"),
        _ = sigterm.recv() => info!("received sigterm"),
        }
        sender.cancel();
    });

    // Trap sighup to support manual file reloading
    let mut sighup = signal(SignalKind::hangup()).unwrap();
    // This task is designed to asynchronously build backend configurations,
    // which may in turn come from other data sources or discovery sources.
    // This inherently races with bringing up servers, to the point where a
    // server may not have any backends to dispatch to yet, if discovery is
    // very slow. This is the intended state, as configuration of processors
    // and any buffers should have already been performed.
    //
    // SIGHUP will attempt to reload backend configurations as well as any
    // discovery changes.
    let discovery_backends = backends.clone();
    tokio::spawn(async move {
        let dconfig = config.discovery.unwrap_or_default();
        let discovery_cache = discovery::Cache::new();
        let mut discovery_stream =
            discovery::reflector(discovery_cache.clone(), discovery::as_stream(&dconfig));
        loop {
            info!("loading configuration and updating backends");
            backend_reloads.inc();
            let config =
                load_backend_configs(&discovery_cache, &discovery_backends, opts.config.as_ref())
                    .await
                    .unwrap();
            let dconfig = config.discovery.unwrap_or_default();

            tokio::select! {
                _ = sighup.recv() => {
                    info!("received sighup");
                    discovery_stream = discovery::reflector(discovery_cache.clone(), discovery::as_stream(&dconfig));
                    info!("reloaded discovery stream");
                }
                Some(event) = discovery_stream.next() => {
                    info!("updating discovery for map {}", event.0);
                }
            };
        }
    });

    // Start processing processor tickers
    let ticker_backends = backends.clone();
    tokio::spawn(backends::ticker(tripwire.clone(), ticker_backends));

    // Wait for the server to finish
    while let Some(name) = run.next().await {
        debug!("server {} exited", name)
    }
    debug!("forcing processor tick to flush");
    backends.processor_tick(std::time::SystemTime::now());
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let opts = Options::from_args();

    info!(
        "statsrelay loading - {} - {}",
        statsrelay::built_info::PKG_VERSION,
        statsrelay::built_info::GIT_COMMIT_HASH.unwrap_or("unknown")
    );

    let config = statsrelay::config::load(opts.config.as_ref())
        .with_context(|| format!("can't load config file from {}", opts.config))?;
    info!("loaded config file {}", opts.config);
    debug!("servers defined: {:?}", config.statsd.servers);
    if opts.config_check {
        info!("--config-check-and-exit set, exiting");
        return Ok(());
    }

    let collector = stats::Collector::default();

    if let Some(admin) = &config.admin {
        admin::spawn_admin_server(admin.port, collector.clone());
        info!("spawned admin server on port {}", admin.port);
    }
    debug!("installed metrics receiver");

    let mut builder = match opts.threaded {
        true => runtime::Builder::new_multi_thread(),
        false => runtime::Builder::new_current_thread(),
    };

    let runtime = builder.enable_all().build().unwrap();
    info!("tokio runtime built, threaded: {}", opts.threaded);

    let scope = collector.scope("statsrelay");

    runtime.block_on(server(scope, config, opts));

    drop(runtime);
    info!("runtime terminated");
    Ok(())
}

/// Load processors from a given config structure and pack them into the given
/// backend set. Currently processors can't be reloaded at runtime.
async fn load_processors(
    scope: Scope,
    backends: &backends::Backends,
    processors: &HashMap<String, config::Processor>,
) -> anyhow::Result<()> {
    for (name, cp) in processors.iter() {
        let proc: Box<dyn processors::Processor + Send + Sync> = match cp {
            config::Processor::TagConverter(tc) => {
                info!("processor tag_converter: {:?}", tc);
                Box::new(processors::tag::Normalizer::new(tc.route.as_ref()))
            }
            config::Processor::Sampler(sampler) => {
                info!("processor sampler: {:?}", sampler);
                Box::new(processors::sampler::Sampler::new(sampler)?)
            }
            config::Processor::Cardinality(cardinality) => {
                info!("processor cardinality: {:?}", cardinality);
                Box::new(processors::cardinality::Cardinality::new(
                    scope.scope(name),
                    cardinality,
                ))
            }
        };
        backends.replace_processor(name.as_str(), proc)?;
    }
    Ok(())
}

async fn load_backend_configs(
    discovery_cache: &discovery::Cache,
    backends: &backends::Backends,
    path: &str,
) -> anyhow::Result<config::Config> {
    // Check if we have to load the configuration file
    let config = match statsrelay::config::load(path)
        .with_context(|| format!("can't load config file from {}", path))
    {
        Err(e) => {
            error!("failed to reload configuration: {}", e);
            return Err(e).context("failed to reload configuration file");
        }
        Ok(ok) => ok,
    };

    let duplicate = &config.statsd.backends;
    for (name, dp) in duplicate.iter() {
        let discovery_data = if let Some(discovery_name) = &dp.shard_map_source {
            discovery_cache.get(discovery_name)
        } else {
            None
        };
        if let Err(e) = backends.replace_statsd_backend(name, dp, discovery_data.as_ref()) {
            error!("failed to replace backend index {} error {}", name, e);
            continue;
        }
    }
    let existing_backends = backends.backend_names();
    let config_backends: HashSet<String> = duplicate.keys().cloned().collect();
    let difference = existing_backends.difference(&config_backends);
    for remove in difference {
        if let Err(e) = backends.remove_statsd_backend(remove) {
            error!("failed to remove backend {} with error {:?}", remove, e);
        }
    }

    info!("backends reloaded");
    Ok(config)
}
