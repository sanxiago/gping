use pinger::{ping, PingOptions};
use std::env;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: tcp_ping <host> [port] [allow_rst]");
        return;
    }

    let host = &args[1];

    // Optional port argument (default 80)
    let port: u16 = if args.len() >= 3 {
        args[2].parse().expect("Port must be a number")
    } else {
        80
    };

    // Optional allow_rst argument (default false)
    let allow_rst: bool = if args.len() >= 4 {
        match args[3].to_lowercase().as_str() {
            "true" | "1" | "yes" => true,
            _ => false,
        }
    } else {
        false
    };

    let opts = PingOptions::new(host, Duration::from_secs(1), None)
        .with_tcping(true)       // enable TCP ping
        .with_port(port)         // set port
        .with_allow_rst(allow_rst); // set allow_rst

    let rx = ping(opts).expect("Failed to start TCP ping");

    for result in rx {
        match result {
            pinger::PingResult::Pong(dur, target) => {
                println!("PONG {} in {:?}", target, dur);
            }
            pinger::PingResult::Timeout(target) => {
                println!("TIMEOUT {}", target);
            }
            pinger::PingResult::Unknown(target) => {
                println!("UNKNOWN {}", target);
            }
            pinger::PingResult::PingExited(_, _) => {}
        }
    }
}
