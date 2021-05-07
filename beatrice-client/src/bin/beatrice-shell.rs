use anyhow::{anyhow, Result};
use beatrice_client::{configuration::Configuration, shell::Shell};
use beatrice_proto::beatrice::beatrice_client::BeatriceClient;
use clap::{AppSettings, Clap};
use std::{fs::File, io::BufReader, path::Path};
use tracing_subscriber::{fmt::format::DefaultFields, prelude::*, EnvFilter};
#[derive(Clap)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(long)]
    conf: String,
    id: u32,
}

fn init_tracing_subscriber() {
    let formatter = DefaultFields::new().delimited(",");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(formatter)
        .try_init();
}

fn load_conf<P: AsRef<Path>>(path: P) -> Result<Configuration> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let conf = serde_yaml::from_reader(reader)?;
    Ok(conf)
}

async fn run(conf_path: &String, id: u32) -> Result<()> {
    let mut conf = load_conf(&conf_path)?;
    let node_conf = conf
        .repc
        .nodes
        .remove(&id)
        .ok_or_else(|| anyhow!("node with id {} does not exist", id))?;
    let channel = node_conf.to_channel_lazy()?;
    let client = BeatriceClient::new(channel);
    let shell = Shell::new(client);
    shell.run().await
}

#[tokio::main]
async fn main() -> Result<()> {
    let Opts { conf, id } = Opts::parse();

    init_tracing_subscriber();

    run(&conf, id).await.map_err(|e| {
        tracing::error!(
            error = <String as AsRef<str>>::as_ref(&e.to_string()),
            "failed to run"
        );
        e
    })
}
