use anyhow::Result;
use beatrice_server::{configuration::Configuration, BeatriceState, BeatriceStateMachine};
use clap::{AppSettings, Clap};
use repc::group::grpc::GrpcRepcGroup;
use std::{fs::File, io::BufReader, path::Path};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt::format::DefaultFields, EnvFilter};

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

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();

    init_tracing_subscriber();

    let id = opts.id;
    let conf = match load_conf(&opts.conf) {
        Ok(conf) => conf,
        Err(e) => {
            tracing::error!(
                error = <String as AsRef<str>>::as_ref(&e.to_string()),
                file = <String as AsRef<str>>::as_ref(&opts.conf),
                "failed to load configuration from file",
            );
            return Err(e);
        }
    };

    let Configuration { repc: conf } = conf;

    let state = BeatriceState::new();
    let state_machine = BeatriceStateMachine::new(state);
    let group = GrpcRepcGroup::new(id, conf, state_machine);

    if let Err(e) = group.run().await {
        tracing::error!(
            error = <String as AsRef<str>>::as_ref(&e.to_string()),
            "failed to start",
        );
        return Err(e.into());
    }
    Ok(())
}
