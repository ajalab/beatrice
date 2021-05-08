mod command;
mod parser;
mod prompter;

use self::{
    command::Command,
    parser::Parser,
    prompter::{InteractivePrompter, Prompter},
};
use anyhow::Result;
use beatrice_proto::beatrice::{
    beatrice_client::BeatriceClient, DeleteRequest, FlushRequest, GetRequest, PutRequest,
};
use bytes::Bytes;
use tonic::transport::Channel;

pub struct Shell<P = InteractivePrompter> {
    client: BeatriceClient<Channel>,
    prompter: P,
    parser: Parser,
}

impl Shell {
    pub fn new(client: BeatriceClient<Channel>) -> Self {
        Self {
            client,
            prompter: InteractivePrompter::new(),
            parser: Parser::new(),
        }
    }
}

impl<P> Shell<P>
where
    P: Prompter,
{
    pub async fn run(mut self) -> Result<()> {
        loop {
            let command = match self.prompter.prompt() {
                Ok(Some(command)) => command,
                Ok(None) => {
                    println!("");
                    break;
                }
                Err(e) => {
                    println!("failed to read command from prompt: {}", e);
                    break;
                }
            };
            let command = match self.parser.parse(command) {
                Ok(command) => command,
                Err(e) => {
                    if !matches!(e, parser::ParseError::Empty) {
                        println!("failed to run command: {}", e);
                    }
                    continue;
                }
            };
            match command {
                Command::Put {
                    row,
                    timestamp,
                    value,
                } => self.put(row, timestamp, value).await,
                Command::Get { row } => self.get(row).await,
                Command::Delete { row, timestamp } => self.delete(row, timestamp).await,
                Command::Flush { cache } => self.flush(cache).await,
                Command::Exit => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn put(&mut self, row: Bytes, timestamp: Option<u64>, value: Bytes) {
        let req = PutRequest {
            row: row.to_vec(),
            timestamp: timestamp.unwrap_or(0),
            value: value.to_vec(),
        };
        if let Err(status) = self.client.put(req).await {
            self.prompter.print_error(status.to_string());
        }
    }

    async fn delete(&mut self, row: Bytes, timestamp: Option<u64>) {
        let req = DeleteRequest {
            row: row.to_vec(),
            timestamp: timestamp.unwrap_or(0),
        };
        if let Err(status) = self.client.delete(req).await {
            self.prompter.print_error(status.to_string());
        }
    }

    async fn get(&mut self, row: Bytes) {
        let req = GetRequest { row: row.to_vec() };

        match self.client.get(req).await {
            Ok(res) => {
                self.prompter.print_result(res.into_inner().value);
            }
            Err(status) => {
                self.prompter.print_error(status.to_string());
            }
        }
    }

    async fn flush(&mut self, cache: bool) {
        let req = FlushRequest { cache };
        let res = self.client.flush(req).await;
        println!("=> {:?}", res);
    }
}
