use database_engine::KVEngine;

use std::io::{self, IsTerminal, Write};
use std::{format, println};

enum Command {
    Put(String, String),
    Delete(String),
    Get(String),
    Exit,
    Stats,
}

// parses one line from the terminal
fn parse(line: &str) -> Result<Command, String> {
    let line = line.trim();
    let (command, rest) = line.split_once(char::is_whitespace).unwrap_or((line, ""));
    let rest = rest.trim_start();

    match command.to_ascii_lowercase().as_str() {
        "put" => match rest.split_once(char::is_whitespace) {
            Some((key, value)) => Ok(Command::Put(
                key.to_string(),
                value.trim_start().to_string(),
            )),
            None => Err(format!(
                "Invalid put command, line passed to repl was: {line}. Expected format: put <key> <value>"
            )),
        },
        "del" | "delete" => Ok(Command::Delete(validate_key(rest, command)?)),
        "get" => Ok(Command::Get(validate_key(rest, command)?)),
        "exit" | "quit" => Ok(Command::Exit),
        "stats" => Ok(Command::Stats),
        _ => Err(format!(
            "Invalid command passed. line passed to repl was: {line}"
        )),
    }
}

/// rejects empty keys or more than one keys
fn validate_key(k: &str, command: &str) -> Result<String, String> {
    if !k.is_empty() && !k.contains(char::is_whitespace) {
        Ok(k.to_string())
    } else {
        Err(format!("Invalid usage. Expected format: <{command}> <key>"))
    }
}

pub(crate) fn run_repl(db: &mut KVEngine) -> io::Result<()> {
    let is_terminal = io::stdin().is_terminal();
    let mut lines = io::stdin().lines();

    loop {
        if is_terminal {
            print!("> ");
            io::stdout().flush()?;
        }

        let Some(line) = lines.next() else {
            break;
        };
        let line = line?;
        let command = match parse(&line) {
            Ok(command) => command,
            Err(err) => {
                println!("{err}");
                continue;
            }
        };

        let result = match command {
            Command::Delete(k) => db.delete(k.as_bytes()).map(|_| println!("OK")),
            Command::Put(k, v) => db.put(k.as_bytes(), v.as_bytes()).map(|_| println!("OK")),
            Command::Exit => {
                break;
            }
            Command::Get(k) => db.get(&k.into_bytes()).map(|val| match val {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(None)"),
            }),
            Command::Stats => db.maintenance().map(|_| println!("{:?}", db.stats())),
        };

        if let Err(e) = result {
            println!("error: {}", e)
        }
    }

    Ok(())
}
