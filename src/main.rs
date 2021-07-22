use clap::{App, Arg};
use iron_carrier::config::Config;
use std::{path::PathBuf, process::exit};

fn main() {
    let matches = App::new("Iron Carrier")
        .version("0.1")
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
        .get_matches();

    let verbosity = matches.occurrences_of("v") as usize;
    // let auto_exit = matches.is_present("auto-exit");

    stderrlog::new()
        .module(module_path!())
        .verbosity(verbosity)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let config = match matches.value_of("config") {
        Some(config) => config.into(),
        None => {
            let mut config_dir = get_project_dir();
            config_dir.push("config.toml");

            config_dir
        }
    };
    let config = match Config::new(&config) {
        Ok(config) => config,
        Err(e) => {
            log::error!("{}", e);
            exit(-1)
        }
    };

    if let Err(e) = iron_carrier::run(config) {
        log::error!("{}", e);
        exit(-1)
    };
}

fn get_project_dir() -> PathBuf {
    let mut config_path = dirs::config_dir().expect("Failed to access home folder");
    config_path.push(".iron_carrier");

    if !config_path.exists() {
        std::fs::create_dir(&config_path).expect("Failed to create Iron Carrier folder");
    }

    config_path
}
