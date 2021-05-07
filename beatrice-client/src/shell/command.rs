use bytes::Bytes;

pub enum Command {
    Put {
        row: Bytes,
        timestamp: Option<u64>,
        value: Bytes,
    },
    Get {
        row: Bytes,
    },
    Flush {
        cache: bool,
    },
    Exit,
}
