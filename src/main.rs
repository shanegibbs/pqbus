extern crate pqbus;

use std::io::{self, Read};
use std::env;

fn run(db_uri: &String, cmd: &String) -> Result<i32, pqbus::error::Error> {
    let bus = try!(pqbus::new(db_uri.as_ref(), "test"));

    match cmd.as_ref() {
        "push" => {
            // read stdin
            let mut buffer = String::new();
            match io::stdin().read_to_string(&mut buffer) {
                Ok(_) => (),
                Err(e) => {
                    println!("Failed to read stdin: {}", e);
                    std::process::exit(1);
                }
            }

            // push message
            let queue = try!(bus.queue("checker"));
            try!(queue.push(buffer));
            println!("Message sent");
        }

        "pop" => {
            // pop message
            let queue = try!(bus.queue("checker"));
            let body = try!(queue.pop_blocking());
            println!("Received: {}", body);
        }

        "popall" => {
            // pop message callback
            let queue = try!(bus.queue("checker"));
            try!(queue.pop_callback(|body| {
                println!("Got: {}", body);
            }));
        }

        _ => {
            println!("Unknown command: {}", cmd);
            return Ok(1);
        }
    }
    Ok(0)
}

fn main() {
    let mut args = env::args();
    args.next(); // skip first

    let cmd = args.next().unwrap_or("".to_string());
    let db_uri = args.next().unwrap_or("".to_string());

    match run(&db_uri, &cmd) {
        Ok(i) => std::process::exit(i),
        Err(e) => {
            println!("Error: {:?}", e);
            std::process::exit(0);
        }
    }
}
