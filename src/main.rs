use clap::Parser;
use iron_carrier::{config::Config, constants::VERSION, leak::Leak, validation::Validated};
use std::{path::PathBuf, process::exit};

#[derive(Parser)]
#[command(name="Iron Carrier", version=VERSION, author="Ilson Balliego <ilson.balliego@gmail.com>", about="Synchronize your files")]
struct Cli {
    #[arg(value_name = "CONFIG", help = "Path to the config file to use")]
    config: Option<String>,

    #[arg(short, long, help = "Starts on daemon mode")]
    daemon: bool,

    #[arg(short, action = clap::ArgAction::Count, help = "Sets the level of verbosity")]
    verbose: u8,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    stderrlog::new()
        .module(module_path!())
        .verbosity(cli.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let config = get_config(cli.config.as_deref()).leak();

    let execution_result = if cli.daemon {
        iron_carrier::start_daemon(config, None).await
    } else {
        iron_carrier::run_full_sync(config).await
    };

    if let Err(e) = execution_result {
        if !e.is::<iron_carrier::StateMachineError>() {
            log::error!("{e}");
        }
        exit(-1)
    };
}

fn get_config(config_path: Option<&str>) -> Validated<Config> {
    let config = match config_path {
        Some(config) => config.into(),
        None => get_project_dir().join("config.toml"),
    };

    match Config::new(&config).and_then(|config| config.validate()) {
        Ok(config) => config,
        Err(err) => {
            log::error!("{err}");
            log::error!("Error reading config at {}", config.to_str().unwrap());
            exit(-1)
        }
    }
}

fn get_project_dir() -> PathBuf {
    let mut config_path = dirs::config_dir().expect("Failed to access home folder");
    config_path.push("iron-carrier");

    if !config_path.exists() {
        std::fs::create_dir(&config_path).expect("Failed to create Iron Carrier folder");
    }

    config_path
}
