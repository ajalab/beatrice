use bytes::{Buf, BufMut, Bytes};
use std::mem;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum Value {
    Val(Bytes),
    Del,
}

impl Value {
    const VAL: u8 = 0;
    const DEL: u8 = 1;

    pub fn size(&self) -> usize {
        match self {
            Value::Val(v) => mem::size_of::<u8>() + mem::size_of::<u64>() + v.len(),
            Value::Del => mem::size_of::<u8>(),
        }
    }

    pub fn write_to<T: BufMut>(self, buf: &mut T) -> usize {
        let size = self.size();
        match self {
            Value::Val(v) => {
                buf.put_u8(Self::VAL);
                buf.put_u64_le(v.len() as u64);
                buf.put(v);
            }
            Value::Del => {
                buf.put_u8(Self::DEL);
            }
        }
        size
    }

    pub fn read_from(buf: &mut Bytes) -> Self {
        // TODO: detect corruption
        let marker = buf.get_u8();
        match marker {
            Self::VAL => {
                let len = buf.get_u64_le() as usize;
                let val = buf.slice(..len);
                buf.advance(len);

                Self::Val(val)
            }
            _ => Self::Del,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    #[test]
    fn test_read_write_val() {
        let mut buf = BytesMut::new();
        let value = Value::Val(Bytes::from("this is a test value"));
        value.clone().write_to(&mut buf);

        let mut buf = buf.freeze();
        let v = Value::read_from(&mut buf);

        assert_eq!(value, v);
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn test_read_write_del() {
        let mut buf = BytesMut::new();
        let value = Value::Del;
        value.clone().write_to(&mut buf);

        let mut buf = buf.freeze();
        let v = Value::read_from(&mut buf);

        assert_eq!(value, v);
        assert_eq!(buf.remaining(), 0);
    }
}
