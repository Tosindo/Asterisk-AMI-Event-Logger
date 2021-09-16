use std::{collections::HashMap, fs::{self, File}, io::{prelude::*, BufReader}, net::TcpStream, sync::mpsc::{self, Sender}, thread};
use serde::{Serialize};
use chrono::{Utc};

use crate::settings::Settings;

mod settings;

// So we are interested in connecting to the AMI server and get all the events into a "log" file.
// We will use the AMI protocol to do this.
// The AMI protocol is quite simple, its based on the HTML header, each message ends with a line containing only a carriage return.

// This function will read from the TCP stream until it finds a line with only a carriage return.
// It will then return all the lines but the last one.
fn read_ami(stream: &mut TcpStream, first: bool) -> AMIResponse {
    let mut ami_response = AMIResponse {
        headers: HashMap::new(),
        rest: String::from(""),
    };

    let mut line = String::new();

    let mut reader = BufReader::new(stream);
    loop {
        line.clear();
        let res = reader.read_line(&mut line);

        match res {
            Ok(s) => {
                if s == 0 {
                    break;
                }

                if line == "\r\n" {
                    break
                }
                
                // Lets check if the line contains a : and if it does, we will split it into the name and value for a header.
                if line.contains(":") {
                    let mut split = line.splitn(2, ":");
                    let name = split.next().unwrap();
                    let value = split.next().unwrap();
                    ami_response.headers.insert(
                        name.trim().to_owned(),
                        value.trim().to_owned()
                    );
                }
                else {
                    // Just add it to the rest of the response.
                    ami_response.rest.push_str(&line);
                }

                // If it is the first line, there is no need to check for the end of the message.
                if first {
                    break;
                }
            },
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }

    }
    ami_response
}


#[derive(Debug, Serialize)]
struct AMIResponse {
    headers: HashMap<String, String>,
    rest: String,
}


fn listener(server: settings::Server, sender: Sender<(String, String)>) {
        // Lets start a TCP connection to the AMI server.
        let mut stream = TcpStream::connect(format!("{}:{}", server.host, server.port)).unwrap();

        let first_response = read_ami(&mut stream, true);

        // Lets check if the first response contains the correct rest data.
        // @TODO implement better error handling.
        assert_eq!(first_response.rest, "Asterisk Call Manager/1.1\r\n");

        // Lets write in the LOGIN command.
        stream.write(b"Action: Login\r\n").unwrap();
        write!(stream, "Username: {}\r\n", server.username).unwrap();
        write!(stream, "Secret: {}\r\n", server.password).unwrap();
        stream.write(b"\r\n").unwrap();

        // Lets get the login response.
        let login_response = read_ami(&mut stream, false);
        // The "Response" header must be "Success".
        // @TODO Again, implement better error handling.
        assert_eq!(login_response.headers.get("Response").unwrap(), "Success");

        loop {
            let ami_response = read_ami(&mut stream, false);
            let time = Utc::now();
            if ami_response.headers.len() > 0 {
                // Lets check if the response contains the "Event" header.
                // If it does we will print TIMESTAMP::JSON_RESPONSE.
                if ami_response.headers.contains_key("Event") {
                    sender.send(
                        (server.name.clone(),
                        format!(
                            "{}::{}::{}\r\n", 
                            server.name, 
                            time.timestamp_millis(), 
                            serde_json::to_string(&ami_response).unwrap()
                        ))
                    ).unwrap();
                }
            }
        }
}

fn get_current_file_name() -> String {
    // The name of the file will be:
    // "events_YYYY-MM-DD.log"
    let mut file_name = String::new();
    file_name.push_str(&format!("events_{}.log", Utc::now().date()));

    file_name
}

fn open_file(path: String) -> File {
    std::fs::OpenOptions::new()
    .write(true)
    .create(true)
    .append(true)
    .open(&path)
    .unwrap()
}

fn main() {
    // Lets get the settings from the settings module.
    let mut settings = match Settings::init() {
        Ok(settings) => settings,
        Err(e) => {
            println!("Error: {}", e);
            return;
        }
    };

    // Lets check if the file path end with a /.
    // If it does lets remove it.
    if settings.basic.target_directory.ends_with("/") {
        settings.basic.target_directory = settings.basic.target_directory[..(settings.basic.target_directory.len() - 1)].to_string();
    }

    // Unmutable the settings.
    let settings = settings;


    let mut handles = vec![];
    
    let (sender, receiver) = mpsc::channel::<(String, String)>();

    // Lets loop the server list and connect to each one on different threads.
    for server in &settings.servers {
        println!("Connecting to {}", server.host);

        let sender1 = sender.clone();
        let server1 = server.clone();
        
        handles.push(thread::spawn(move || {
            listener(server1, sender1);
        }));
    }

    // Lets make sure we have a path to our settings.basic.target_directory:
    let target_directory = settings.basic.target_directory.clone();
    if target_directory.len() == 0 {
        println!("Error: No target directory specified.");
        return;
    }
    else {
        fs::create_dir_all(target_directory).unwrap();
    }


    let mut server_paths: HashMap<String, String> = HashMap::new();

    // We want to check if directory_per_server is true in the settings if so we will create a directory for each server, and create a hashmap holding the file.
    if settings.basic.directory_per_server {
        for server in &settings.servers {
            let dir = format!("{}/{}", &settings.basic.target_directory, server.name);
            println!("Creating directory {}", dir);
            fs::create_dir_all(&dir).unwrap();

            server_paths.insert(server.name.clone(), dir);
        }
    }

    
    let mut files: HashMap<String, File> = HashMap::new();
    let mut event_file_name = String::from("");
    let all = String::from("all");

    loop {
        let (server_name, msg) = match receiver.recv() {
            Ok((server_name, msg)) => (server_name, msg),
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        };

        let mut file: &File;

        // Lets check if the file name changed.
        if event_file_name != get_current_file_name() {
            event_file_name = get_current_file_name();

            // We need to update all the files for each server, or not depending on the settings.
            if settings.basic.directory_per_server {
                for server in &settings.servers {
                    files.insert(server.name.clone(), 
                        open_file(format!("{}/{}", &server_paths.get(&server.name).unwrap(), event_file_name))
                    );
                }
            }
            else {
                files.insert(all.clone(),
                    open_file(format!("{}/{}", &settings.basic.target_directory, event_file_name))
                );
            }
        }

        // Now lets get the target file for the current server.
        if settings.basic.directory_per_server {
            file = files.get(&server_name).unwrap();
        } else {
            file = files.get(&all).unwrap();
        }

        // Lets write the message to the events file.
        file.write_all(msg.as_bytes()).unwrap();
    }

    // Lets wait for all the threads to finish.
    for handle in handles {
        handle.join().unwrap();
    }
}
