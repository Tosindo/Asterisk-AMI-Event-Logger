use std::{collections::HashMap, fs::{self, File}, io::{prelude::*, BufReader}, net::TcpStream, sync::mpsc::{self, Sender}, thread};
use serde::{Serialize};
use chrono::{Utc};
use mysql::{Opts, Pool, prelude::Queryable};

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


fn listener(server: settings::Server, sender: Sender<(String, AMIResponse)>) {
        // Lets start a TCP connection to the AMI server.
        let mut stream = match TcpStream::connect(format!("{}:{}", server.host, server.port)) {
            Ok(stream) => stream,
            Err(e) => {
                println!("Unable to connect to TCP of server {}, {}:{}, with error: {}.", server.name, server.host, server.port, e);
                return;
            }
        };

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

        match login_response.headers.get("Response") {
            Some(response) => {
                if response != "Success" {
                    println!("Login failed for server {}, with response: {}.", server.name, response);
                    return;
                }
            },
            None => {
                println!("Unable to get login response while connecting to server {}.", server.name);
                return;
            }
        }

        loop {
            let ami_response = read_ami(&mut stream, false);
            if ami_response.headers.len() > 0 {
                // Lets check if the response contains the "Event" header.
                // If it does we will print TIMESTAMP::JSON_RESPONSE.
                if ami_response.headers.contains_key("Event") {
                    sender.send(
                        (server.name.clone(),
                        ami_response
                    )
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
    
    let (sender, receiver) = mpsc::channel::<(String, AMIResponse)>();

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

    // This hashmap will hold all mysql pools.
    let mut mysql_pool = HashMap::new();
    // Lets loop settings.databases and create a connection for each one.
    for database in &settings.databases {
        println!("Connecting to MySQL database {}.", database.host);

        // Lets first check if this database is already in the hashmap, if it is, it means there are duplicates in the settings, so we will error out.
        if mysql_pool.contains_key(&database.host) {
            println!("Database {} is already connected.", database.host);
            return;
        }

        let url = format!("mysql://{}:{}@{}:{}/{}", database.user, database.password, database.host, database.port, database.database);
        let opts = match Opts::from_url(&url) {
            Ok(opts) => opts,
            Err(e) => {
                println!("Unable to connect to MySQL database {} with error: {}", database.host, e);
                continue;
            }
        };

        let pool = match Pool::new(opts) {
            Ok(pool) => pool,
            Err(e) => {
                println!("Unable to connect to MySQL database {} with error: {}", database.host, e);
                continue;
            }
        };


        mysql_pool.insert(database.id.clone(), pool);

        println!("Connected successfully to database {}.", database.host);
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
        let (server_name, ami_response) = match receiver.recv() {
            Ok((server_name, ami_response)) => (server_name, ami_response),
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        };

        // Now lets check if the event name matches any in the settings.event_clauses[event_name]
        // If it does we will write the event to the database.
        for event_clause in &settings.event_clauses {
            if &event_clause.event_name == ami_response.headers.get("Event").unwrap() {
                // So now we have a match, so we get the db pool from the db_connection_id, and target table from db_table.
                let pool = mysql_pool.get(&event_clause.db_connection_id).unwrap();
                let table = event_clause.db_table.clone();

                // Now inside the event_clause we have a HashMap named event_data_link that will match the headers of the event to the database columns.
                // So now we need to prepare the SQL statement, and the vector that will hold the values.
                let mut columns = vec![];
                let mut values = vec![];

                for (event_key, mysql_column) in &event_clause.event_data_link {
                    // Lets check if the event_key is in the ami_response.headers.
                    if ami_response.headers.contains_key(event_key) {
                        // If it is we will add the value to the values hashmap.
                        values.push(mysql::Value::from(ami_response.headers.get(event_key)));
                    } else {
                        match event_key.as_str() {
                            "%SERVER_NAME%" => {
                                // If the event_key is %SERVER_NAME% we will add the server_name to the values hashmap.
                                values.push(mysql::Value::from(&server_name));
                            },
                            _ => {
                                values.push(mysql::Value::from(None::<String>));
                            }
                        }
                    }
                    // And add the column name to the columns.
                    columns.push(mysql_column.clone());
                }

                // Now we have the columns and values, lets prepare the SQL statement.
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})", 
                    table, 
                    // We want all columns separated by commas.
                    &columns.join(","), 
                    // Now we want ? for each column or value.
                    vec!["?"; columns.len()].join(",")
                );


                let mut conn = pool.get_conn().unwrap();
                let _s: Vec<mysql::Row> = match conn.exec(sql, values) {
                    Ok(s) =>  {
                        println!("Successfully inserted row into database {} table {}.", &event_clause.db_connection_id, &event_clause.db_table);
                        s
                    },
                    Err(e) => {
                        println!("Unable to insert row into database {} table {} with error: {}", &event_clause.db_connection_id, &event_clause.db_table, e);
                        continue;
                    }
                };
            }
        }

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

        let time = Utc::now();

        let msg = 
        format!(
            "{}::{}::{}\r\n", 
            server_name, 
            time.timestamp_millis(), 
            serde_json::to_string(&ami_response).unwrap()
        );

        // Lets write the message to the events file.
        file.write_all(msg.as_bytes()).unwrap();
    }

    // Lets wait for all the threads to finish.
    for handle in handles {
        handle.join().unwrap();
    }
}
