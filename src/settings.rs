use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::Path, fmt, error::Error, fs::OpenOptions, io::{Read, Write}};


#[derive(Debug)]
pub enum SettingsError {
    ParseError(String),
    WriteError,
    ReadError,
}

impl Error for SettingsError {}

impl Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SettingsError::ParseError(msg) => {
                write!(f, "Unable to parse settings file: {}", msg)
            },
            SettingsError::WriteError => {
                write!(f, "Unable to write to settings file.")
            },
            SettingsError::ReadError => {
                write!(f, "Unable to read from settings file.")
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Settings {
    pub basic: Basic,
    pub servers: Vec<Server>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Basic {
    pub target_directory: String,
    pub directory_per_server: bool
}

const SETTINGS_FILE: &str = "settings.toml";

impl Settings {
    // On init we will get settings from the config file.
    // If it doesnt exist we create a new one and place in the default settings.
    // The settings file is stored in toml format, if there is an error parsing we will print the error and exit.
    pub fn init() -> Result<Settings, SettingsError>  {
        // Lets check if the file exists:
        let settings_file = Path::new(SETTINGS_FILE);
        if !settings_file.exists() {
            let settings = Settings::default();

            // Lets convert the settings to a toml string and write it to the file.
            let toml = match toml::to_string(&settings) {
                Ok(str) => str,
                Err(_e) => {
                    return Err(SettingsError::WriteError);
                }
            };
            
            // Lets open and write our file with OpenOptions.
            let mut f = match OpenOptions::new()
                .write(true)
                .create(true)
                .open(SETTINGS_FILE) {
                    Ok(f) => f,
                    Err(_e) => {
                        return Err(SettingsError::WriteError);
                    }
                };
            
            // Lets write our toml string to the file.
            match write!(f, "{}", toml) {
                Ok(_) => {
                    return Ok(settings);
                },
                Err(_e) => {
                    return Err(SettingsError::WriteError);
                }
            };
        }
        else {
            // Lets read the settings file.
            let mut f = match OpenOptions::new()
                .read(true)
                .open(SETTINGS_FILE) {
                    Ok(f) => f,
                    Err(_e) => {
                        return Err(SettingsError::ReadError);
                    }
                };
            
            // Lets parse the settings file.
            let mut toml = String::from("");
            let _size = f.read_to_string(&mut toml);
            let settings:Settings = match toml::from_str(&mut toml) {
                Ok(settings) => settings,
                Err(e) => {
                    return Err(SettingsError::ParseError(e.to_string()));
                }
            };
            Ok(settings)
        }
    }
}

// Represents a AMI Asterisk Server instance to be monitored.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Server {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
}


impl Default for Server {
    fn default() -> Self {
        Server {
            name: String::from("Example"),
            host: String::from("127.0.0.1"),
            port: 5038,
            username: String::from("admin"),
            password: String::from("admin"),
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            basic: Basic::default(),
            servers: vec![
                Server::default(),
            ],
        }
    }
}

impl Default for Basic {
    fn default() -> Self {
        Basic {
            target_directory: String::from("events"),
            directory_per_server: false
        }
    }
}
