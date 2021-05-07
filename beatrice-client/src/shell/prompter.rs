use anyhow::Result;
use std::{
    fmt,
    io::{self, Write},
    str,
};

pub trait Prompter {
    fn prompt(&mut self) -> Result<Option<String>>;
    fn print_result<T: AsRef<[u8]> + fmt::Debug>(&mut self, res: T);
    fn print_error(&mut self, e: String);
}

pub struct InteractivePrompter {}

impl InteractivePrompter {
    const PROMPT: &'static str = "> ";

    pub fn new() -> Self {
        Self {}
    }
}

impl Prompter for InteractivePrompter {
    fn prompt(&mut self) -> Result<Option<String>> {
        let mut command = String::new();

        print!("{}", Self::PROMPT);
        io::stdout().flush()?;

        let n = io::stdin().read_line(&mut command)?;
        match n {
            0 => Ok(None),
            _ => Ok(Some(command)),
        }
    }

    fn print_result<T: AsRef<[u8]> + fmt::Debug>(&mut self, v: T) {
        let s = str::from_utf8(v.as_ref());
        match s {
            Ok(s) => println!("{}", s),
            Err(_) => println!("{:?}", v),
        }
    }

    fn print_error(&mut self, e: String) {
        println!("Error: {}", e);
    }
}
