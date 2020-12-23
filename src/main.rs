use std::process::exit;

use iron_carrier::config::Config;

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    args.next();

    stderrlog::new()
        .module(module_path!())
        .verbosity(5)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();


    let config_path = args.next().expect("You must provide a configuration path");

    let config = match Config::new(config_path) {
        Ok(config) => { config }
        Err(e) => { 
            log::error!("{}", e);
            exit(- 1)
        }
    };

    
    let mut s = iron_carrier::sync::Synchronizer::new(config);
    if let Err(e) = s.start().await {
        log::error!("{}", e);
        exit(- 1)
    };
}
