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
    Delete {
        row: Bytes,
        timestamp: Option<u64>,
    },
    Flush {
        cache: bool,
    },
    Exit,
}
