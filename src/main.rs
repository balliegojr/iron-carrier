use clap::{App, Arg};
use iron_carrier::config::Config;
use std::process::exit;

fn main() {
    let matches = App::new("Iron Carrier")
        .version("0.1")
        .author("Ilson Roberto Balliego Junior <ilson.balliego@gmail.com>")
        .about("Synchronize your files")
        .arg(
            Arg::with_name("config")
                .help("Sets the config file to use")
                .value_name("Config")
                .required(true)
                .takes_value(true),
        )
        // .arg(
        //     Arg::with_name("auto-exit")
        //         .help("Auto exit after sync")
        //         .long("auto-exit")
        //         .short("e"),
        // )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    let config = matches
        .value_of("config")
        .expect("You must provide a configuration path");
    let verbosity = matches.occurrences_of("v") as usize;
    // let auto_exit = matches.is_present("auto-exit");

    stderrlog::new()
        .module(module_path!())
        .verbosity(verbosity)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let config = match Config::new(config) {
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
