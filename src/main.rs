use std::process::exit;

use r_sync::config::Config;


fn print_error(e: r_sync::RSyncError) -> ! {
    eprintln!("{}", e);
    exit(-1);
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    args.next();

    let config_path = args.next().expect("You must provide a configuration path");

    let config = match Config::new(config_path) {
        Ok(config) => { config }
        Err(e) => { print_error(e) }
    };

    
    let mut s = r_sync::sync::Synchronizer::new(config);
    if let Err(e) = s.start().await {
        eprint!("{}", e);
    };
}
