use clap::{App, Arg};
use iron_carrier::{config::Config, constants::VERSION, leak::Leak, validation::Validated};
use std::{path::PathBuf, process::exit};

#[tokio::main]
async fn main() {
    let matches = App::new("Iron Carrier")
        .version(VERSION)
        .author("Ilson Roberto Balliego Junior <ilson.balliego@gmail.com>")
        .about("Synchronize your files")
        .arg(
            Arg::with_name("config")
                .help("Sets the config file to use")
                .value_name("Config")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("daemon")
                .short("d")
                .required(false)
                .help("Runs as a daemon"),
        )
        .get_matches();

    let verbosity = matches.occurrences_of("v") as usize;

    stderrlog::new()
        .module(module_path!())
        .verbosity(verbosity)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let config = get_config(matches.value_of("config")).leak();

    let execution_result = if matches.is_present("daemon") {
        iron_carrier::start_daemon(config, None).await
    } else {
        iron_carrier::run_full_sync(config).await
    };

    if let Err(e) = execution_result {
        log::error!("{e}");
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
