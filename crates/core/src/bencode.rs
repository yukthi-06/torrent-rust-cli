use std::collections::BTreeMap;
use std::str;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BencodeError {
    #[error("Unexpected end of input")]
    UnexpectedEOF,
    #[error("Invalid character: {0}")]
    InvalidChar(char),
    #[error("Invalid integer format")]
    InvalidInteger,
    #[error("Invalid byte string length")]
    InvalidLength,
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] str::Utf8Error),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Bencode {
    Int(i64),
    ByteString(Vec<u8>),
    List(Vec<Bencode>),
    Dict(BTreeMap<Vec<u8>, Bencode>),
}

impl std::fmt::Debug for Bencode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bencode::Int(i) => write!(f, "Int({})", i),
            Bencode::ByteString(bytes) => {
                if let Ok(s) = str::from_utf8(bytes) {
                    write!(f, "Str({:?})", s)
                } else {
                    write!(f, "Bytes({} bytes)", bytes.len())
                }
            }
            Bencode::List(list) => f.debug_list().entries(list).finish(),
            Bencode::Dict(dict) => {
                let mut dbg = f.debug_map();
                for (k, v) in dict {
                    if let Ok(s) = str::from_utf8(k) {
                        dbg.entry(&s, v);
                    } else {
                        dbg.entry(&String::from_utf8_lossy(k), v);
                    }
                }
                dbg.finish()
            }
        }
    }
}

impl Bencode {
    pub fn decode(bytes: &[u8]) -> Result<Self, BencodeError> {
        let mut offset = 0;
        Self::decode_inner(bytes, &mut offset)
    }

    fn decode_inner(bytes: &[u8], offset: &mut usize) -> Result<Self, BencodeError> {
        if *offset >= bytes.len() {
            return Err(BencodeError::UnexpectedEOF);
        }

        match bytes[*offset] as char {
            'i' => {
                *offset += 1;
                let start = *offset;
                while *offset < bytes.len() && bytes[*offset] as char != 'e' {
                    *offset += 1;
                }
                if *offset >= bytes.len() {
                    return Err(BencodeError::UnexpectedEOF);
                }
                let int_str = str::from_utf8(&bytes[start..*offset])?;
                let val = int_str
                    .parse::<i64>()
                    .map_err(|_| BencodeError::InvalidInteger)?;
                *offset += 1; // skip 'e'

                Ok(Bencode::Int(val))
            }
            'l' => {
                *offset += 1;
                let mut list = Vec::new();
                while *offset < bytes.len() && bytes[*offset] as char != 'e' {
                    list.push(Self::decode_inner(bytes, offset)?);
                }
                if *offset >= bytes.len() {
                    return Err(BencodeError::UnexpectedEOF);
                }
                *offset += 1; // skip 'e'
                Ok(Bencode::List(list))
            }
            'd' => {
                *offset += 1;
                let mut dict = BTreeMap::new();
                while *offset < bytes.len() && bytes[*offset] as char != 'e' {
                    let key = match Self::decode_inner(bytes, offset)? {
                        Bencode::ByteString(s) => s,
                        _ => return Err(BencodeError::InvalidChar(bytes[*offset] as char)),
                    };
                    let value = Self::decode_inner(bytes, offset)?;
                    dict.insert(key, value);
                }
                if *offset >= bytes.len() {
                    return Err(BencodeError::UnexpectedEOF);
                }
                *offset += 1; // skip 'e'
                Ok(Bencode::Dict(dict))
            }
            c if c.is_ascii_digit() => {
                let start = *offset;
                while *offset < bytes.len() && bytes[*offset] as char != ':' {
                    *offset += 1;
                }
                if *offset >= bytes.len() {
                    return Err(BencodeError::UnexpectedEOF);
                }
                let len_str = str::from_utf8(&bytes[start..*offset])?;
                let len = len_str
                    .parse::<usize>()
                    .map_err(|_| BencodeError::InvalidLength)?;
                *offset += 1; // skip ':'

                if *offset + len > bytes.len() {
                    return Err(BencodeError::UnexpectedEOF);
                }
                let val = bytes[*offset..*offset + len].to_vec();
                *offset += len;
                Ok(Bencode::ByteString(val))
            }
            c => Err(BencodeError::InvalidChar(c)),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_inner(&mut buf);
        buf
    }

    fn encode_inner(&self, buf: &mut Vec<u8>) {
        match self {
            Bencode::Int(val) => {
                buf.push(b'i');
                buf.extend_from_slice(val.to_string().as_bytes());
                buf.push(b'e');
            }
            Bencode::ByteString(val) => {
                buf.extend_from_slice(val.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(val);
            }
            Bencode::List(list) => {
                buf.push(b'l');
                for item in list {
                    item.encode_inner(buf);
                }
                buf.push(b'e');
            }
            Bencode::Dict(dict) => {
                buf.push(b'd');
                for (k, v) in dict {
                    // write key
                    buf.extend_from_slice(k.len().to_string().as_bytes());
                    buf.push(b':');
                    buf.extend_from_slice(k);
                    // write value
                    v.encode_inner(buf);
                }
                buf.push(b'e');
            }
        }
    }
}

/// Helper function to locate the exact byte slice of the "info" dictionary in the raw torrent file.
pub fn find_info_dict_slice(bytes: &[u8]) -> Option<&[u8]> {
    let mut offset = 0;
    if bytes.is_empty() || bytes[offset] as char != 'd' {
        return None;
    }
    offset += 1;
    while offset < bytes.len() && bytes[offset] as char != 'e' {
        let key_start = offset;
        // Parse key
        if Bencode::decode_inner(bytes, &mut offset).is_err() {
            return None;
        }
        let key_bytes = &bytes[key_start..offset];
        // Check if key is "4:info"
        let is_info = key_bytes == b"4:info";

        let val_start = offset;
        // Parse value
        if Bencode::decode_inner(bytes, &mut offset).is_err() {
            return None;
        }
        if is_info {
            return Some(&bytes[val_start..offset]);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_int() {
        let data = b"i42e";
        let val = Bencode::decode(data).unwrap();
        assert_eq!(val, Bencode::Int(42));
        assert_eq!(val.encode(), data);
    }

    #[test]
    fn test_decode_string() {
        let data = b"4:spam";
        let val = Bencode::decode(data).unwrap();
        assert_eq!(val, Bencode::ByteString(b"spam".to_vec()));
        assert_eq!(val.encode(), data);
    }

    #[test]
    fn test_decode_list() {
        let data = b"l4:spami42ee";
        let val = Bencode::decode(data).unwrap();
        match val {
            Bencode::List(list) => {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Bencode::ByteString(b"spam".to_vec()));
                assert_eq!(list[1], Bencode::Int(42));
            }
            _ => panic!("Expected List"),
        }
        assert_eq!(val.encode(), data);
    }

    #[test]
    fn test_decode_dict() {
        let data = b"d3:bar4:spam3:fooi42ee";
        let val = Bencode::decode(data).unwrap();
        match val {
            Bencode::Dict(dict) => {
                assert_eq!(dict.len(), 2);
                assert_eq!(
                    dict.get(b"bar".as_ref()).unwrap(),
                    &Bencode::ByteString(b"spam".to_vec())
                );
                assert_eq!(dict.get(b"foo".as_ref()).unwrap(), &Bencode::Int(42));
            }
            _ => panic!("Expected Dict"),
        }
        assert_eq!(val.encode(), data);
    }
}

