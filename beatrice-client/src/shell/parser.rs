use super::command::Command;
use bytes::Bytes;
use std::{error, fmt};

pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Parser {}
    }

    pub fn parse(&self, command: String) -> Result<Command, ParseError> {
        self.parse_tokens(command.split_ascii_whitespace())
    }

    fn parse_tokens<'a>(
        &self,
        mut tokens: impl Iterator<Item = &'a str>,
    ) -> Result<Command, ParseError> {
        let cmd = tokens.next().ok_or_else(|| ParseError::Empty)?;

        match cmd {
            "put" => self.parse_put(tokens),
            "get" => self.parse_get(tokens),
            "flush" => self.parse_flush(tokens),
            "exit" => self.parse_exit(tokens),
            _ => Err(ParseError::Unknown(cmd.to_string())),
        }
    }

    fn parse_put<'a>(&self, tokens: impl Iterator<Item = &'a str>) -> Result<Command, ParseError> {
        let mut args = tokens.collect::<Vec<_>>();
        let n = args.len();
        match n {
            2 => {
                let value = Bytes::from(args.pop().unwrap().to_string());
                let row = Bytes::from(args.pop().unwrap().to_string());
                Ok(Command::Put {
                    row,
                    timestamp: None,
                    value,
                })
            }
            3 => {
                let value = Bytes::from(args.pop().unwrap().to_string());
                let timestamp =
                    args.pop()
                        .unwrap()
                        .parse::<u64>()
                        .map_err(|e| ParseError::ArgParseFailed {
                            arg: "timestamp",
                            pos: 2,
                            e: e.into(),
                        })?;
                let row = Bytes::from(args.pop().unwrap().to_string());
                Ok(Command::Put {
                    row,
                    timestamp: Some(timestamp),
                    value,
                })
            }
            _ => Err(ParseError::WrongArgNum {
                cmd: "put",
                expected: 2,
                actual: n,
            }),
        }
    }

    fn parse_get<'a>(&self, tokens: impl Iterator<Item = &'a str>) -> Result<Command, ParseError> {
        let mut args = tokens.collect::<Vec<_>>();
        let n = args.len();
        if n != 1 {
            return Err(ParseError::WrongArgNum {
                cmd: "get",
                expected: 1,
                actual: n,
            });
        }
        let row = Bytes::from(args.pop().unwrap().to_string());

        Ok(Command::Get { row })
    }

    fn parse_flush<'a>(
        &self,
        tokens: impl Iterator<Item = &'a str>,
    ) -> Result<Command, ParseError> {
        let mut args = tokens.collect::<Vec<_>>();
        let n = args.len();
        match n {
            0 => Ok(Command::Flush { cache: true }),
            1 => {
                let cache = args.pop().unwrap().parse::<bool>().map_err(|e| {
                    ParseError::ArgParseFailed {
                        arg: "cache",
                        pos: 1,
                        e: e.into(),
                    }
                })?;
                Ok(Command::Flush { cache })
            }
            _ => Err(ParseError::WrongArgNum {
                cmd: "flush",
                expected: 2,
                actual: n,
            }),
        }
    }

    fn parse_exit<'a>(&self, tokens: impl Iterator<Item = &'a str>) -> Result<Command, ParseError> {
        let n = tokens.count();
        if n != 0 {
            return Err(ParseError::WrongArgNum {
                cmd: "exit",
                expected: 0,
                actual: n,
            });
        }
        Ok(Command::Exit)
    }
}

#[derive(Debug)]
pub enum ParseError {
    Empty,
    Unknown(String),
    WrongArgNum {
        cmd: &'static str,
        expected: usize,
        actual: usize,
    },
    ArgParseFailed {
        arg: &'static str,
        pos: usize,
        e: Box<dyn error::Error + Send + Sync + 'static>,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Empty => write!(f, "no command is specified"),
            ParseError::Unknown(cmd) => write!(f, "unknown command: {}", cmd),
            ParseError::WrongArgNum {
                cmd,
                expected,
                actual,
            } => write!(
                f,
                "command `{}` requires {} arguments, but got {}",
                cmd, expected, actual
            ),
            ParseError::ArgParseFailed { arg, pos, e } => write!(
                f,
                "cannot parse argument `{}` at position {}: error={:?}",
                arg, pos, e
            ),
        }
    }
}

impl error::Error for ParseError {}
