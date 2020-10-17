use std::process::exit;

use r_sync::config::Config;


fn print_error(e: r_sync::RSyncError) -> ! {
    eprintln!("{}", e);
    exit(-1);
}

fn main() {
    let mut args = std::env::args();
    args.next();

    let config_path = if args.len() == 1 {
        args.next().expect("You must provide a configuration path")
    } else {
        "./samples/config_peer_b.toml".to_string()
    };

    let config = match Config::new(config_path) {
        Ok(config) => { config }
        Err(e) => { print_error(e) }
    };

    // let server_handle = r_sync::start_server(config.clone());
    match r_sync::full_sync_available_peers(&config) {
        Ok(_) => { println!("Sync completed! ") }
        Err(e) => { print_error(e) }
    }

    // server_handle.join().unwrap();

}
