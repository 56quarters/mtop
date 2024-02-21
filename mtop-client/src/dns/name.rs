use crate::core::MtopError;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::fmt::Display;
use std::io::{Read, Seek, SeekFrom};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Name {
    labels: Vec<String>,
}

impl Name {
    const MAX_LENGTH: usize = 255;
    const MAX_LABEL_LENGTH: usize = 63;
    const MAX_POINTERS: u32 = 64;

    pub fn root() -> Self {
        Name { labels: Vec::new() }
    }

    pub fn size(&self) -> u16 {
        (self.labels.iter().map(|l| l.len()).sum::<usize>() + self.labels.len()) as u16 + 1
    }

    pub fn is_root(&self) -> bool {
        self.labels.is_empty()
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        for label in self.labels.iter() {
            buf.write_u8(label.len() as u8)?;
            buf.write_all(label.as_bytes())?;
        }

        Ok(buf.write_u8(0)?)
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut parts = Vec::new();
        loop {
            let len = buf.read_u8()?;
            // If the length isn't a length but actually a pointer to another name
            // or label within the message, seek to that position within the message
            // and read the name from there. `read_offset_into` will follow any further
            // offsets and read the labels for the name into `parts`. After resolving
            // all pointers and reading labels, reset the stream back to immediately
            // after the pointer.
            if Self::is_offset(len) {
                let offset = Self::get_offset(len, buf.read_u8()?);
                let current = buf.stream_position()?;
                Self::read_offset_into(&mut buf, offset, &mut parts)?;
                buf.seek(SeekFrom::Start(current))?;
                break;
            }

            // If the length is a length, read the next label (segment) of the name breaking
            // the loop once we read the "root" label (`.`) signified by a length of 0.
            if Self::read_label_into(&mut buf, len, &mut parts)? {
                break;
            }
        }

        String::from_utf8(parts)
            .map_err(|e| MtopError::runtime_cause("invalid name", e))
            .and_then(|s| Self::from_str(&s))
    }

    fn read_offset_into<T>(mut buf: T, offset: u64, out: &mut Vec<u8>) -> Result<(), MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        buf.seek(SeekFrom::Start(offset))?;
        let mut pointers = 1;

        loop {
            // To avoid loops from badly behaved servers, only follow a fixed number of
            // pointers when trying to resolve a single name. This number is picked to be
            // much higher than most names will use but still finite.
            if pointers > Self::MAX_POINTERS {
                return Err(MtopError::runtime(format!(
                    "reached max number of pointers ({}) while reading name",
                    Self::MAX_POINTERS
                )));
            }

            let len = buf.read_u8()?;
            if Self::is_offset(len) {
                // If this length is actually a pointer to another name or label within
                // the message, seek there to read it on the next iteration. We don't
                // bother keeping track of where to seek back to after resolving it because
                // this is unnecessary since we're already resolving a pointer if this
                // method is being called from `read_ne_bytes`.
                let offset = Self::get_offset(len, buf.read_u8()?);
                buf.seek(SeekFrom::Start(offset))?;
                pointers += 1;
                continue;
            }

            // If the length is a length, read the next label (segment) of the name
            // returning once we read the "root" label (`.`) signified by a length of 0.
            if Self::read_label_into(&mut buf, len, out)? {
                return Ok(());
            }
        }
    }

    /// Read the next name label of length `len` into `out` and return true if the
    /// label was the root label (`.`) and this name is complete, false otherwise.
    fn read_label_into<T>(buf: T, len: u8, out: &mut Vec<u8>) -> Result<bool, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        if len == 0 {
            return Ok(true);
        }

        // Only six bits of the length are supposed to be used to encode the
        // length of a label so 63 is the max length but double check just in
        // case one of the pointer bits was set for some reason.
        if len as usize > Self::MAX_LABEL_LENGTH {
            return Err(MtopError::runtime(format!(
                "max size for label would be exceeded reading {} bytes",
                len,
            )));
        }

        if len as usize + out.len() + 1 > Self::MAX_LENGTH {
            return Err(MtopError::runtime(format!(
                "max size for name would be exceeded adding {} bytes to {}",
                len,
                out.len()
            )));
        }

        let mut handle = buf.take(len as u64);
        let n = handle.read_to_end(out)?;
        if n != len as usize {
            return Err(MtopError::runtime(format!(
                "short read for Name segment. expected {} got {}",
                len, n
            )));
        }

        out.push(b'.');
        Ok(false)
    }

    fn is_offset(len: u8) -> bool {
        // The top two bits of the length byte of a name label (section) are used
        // to indicate the name is actually an offset in the DNS message to a previous
        // name to avoid duplicating the same names over and over.
        len & 0b1100_0000 == 192
    }

    fn get_offset(len: u8, next: u8) -> u64 {
        let pointer = ((len & 0b0011_1111) as u16) << 8;
        (pointer | (next as u16)) as u64
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.", self.labels.join("."))
    }
}

impl FromStr for Name {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() || s == "." {
            return Ok(Self::root());
        }

        if s.len() > Self::MAX_LENGTH {
            return Err(MtopError::runtime(format!(
                "Names are limited to {} bytes max: {}",
                Self::MAX_LENGTH,
                s
            )));
        }

        if !s.ends_with('.') {
            return Err(MtopError::runtime(format!(
                "Names must be fully qualified and end with a '.': {}",
                s
            )));
        }

        let mut labels = Vec::new();
        for label in s.trim_end_matches('.').split('.') {
            let len = label.len();
            if len > Self::MAX_LABEL_LENGTH {
                return Err(MtopError::runtime(format!(
                    "Name labels are limited to {} bytes max: {}",
                    Self::MAX_LABEL_LENGTH,
                    label
                )));
            }

            for (i, c) in label.char_indices() {
                if i == 0 && !c.is_ascii_alphanumeric() && c != '_' {
                    return Err(MtopError::runtime(format!(
                        "label must begin with ASCII letter, number, or underscore: {}",
                        label
                    )));
                } else if i == len - 1 && !c.is_ascii_alphanumeric() {
                    return Err(MtopError::runtime(format!(
                        "label must end with ASCII letter or number: {}",
                        label
                    )));
                } else if !c.is_ascii_alphanumeric() && c != '-' && c != '_' {
                    return Err(MtopError::runtime(format!(
                        "label must be ASCII letter, number, hyphen, or underscore: {}",
                        label
                    )));
                }
            }

            labels.push(label.to_lowercase());
        }

        Ok(Name { labels })
    }
}

#[cfg(test)]
mod test {
    use super::Name;
    use std::io::Cursor;
    use std::str::FromStr;

    #[test]
    fn test_name_from_str_max_length() {
        let parts = vec![
            "a".repeat(Name::MAX_LABEL_LENGTH),
            "b".repeat(Name::MAX_LABEL_LENGTH),
            "c".repeat(Name::MAX_LABEL_LENGTH),
            "d".repeat(Name::MAX_LABEL_LENGTH),
            "com.".to_owned(),
        ];
        let res = Name::from_str(&parts.join("."));
        assert!(res.is_err());
    }

    #[test]
    fn test_name_from_str_error_max_label() {
        let parts = vec!["a".repeat(Name::MAX_LABEL_LENGTH + 1), "com.".to_owned()];
        let res = Name::from_str(&parts.join("."));
        assert!(res.is_err());
    }

    #[test]
    fn test_name_from_str_error_bad_label_start() {
        let res = Name::from_str("-example.com.");
        assert!(res.is_err());
    }

    #[test]
    fn test_name_from_str_error_bad_label_end() {
        let res = Name::from_str("example-.com.");
        assert!(res.is_err());
    }

    #[test]
    fn test_name_from_str_error_bad_label_char() {
        let res = Name::from_str("exa%mple.com.");
        assert!(res.is_err());
    }

    #[test]
    fn test_name_from_str_error_not_fqdn() {
        let res = Name::from_str("localhost");
        assert!(res.is_err());
    }

    #[test]
    fn test_name_from_str_success_fqdn() {
        let name = Name::from_str("example.com.").unwrap();
        assert!(!name.is_root());
    }

    #[test]
    fn test_name_from_str_success_root_empty() {
        let name = Name::from_str("").unwrap();
        assert!(name.is_root());
    }

    #[test]
    fn test_name_from_str_success_root_dot() {
        let name = Name::from_str(".").unwrap();
        assert!(name.is_root());
    }

    #[test]
    fn test_name_size_root() {
        let name = Name::root();
        assert_eq!(1, name.size());
    }

    #[test]
    fn test_name_size_non_root() {
        let name = Name::from_str("example.com.").unwrap();
        assert_eq!(13, name.size());
    }

    #[test]
    fn test_name_write_network_bytes_root() {
        let mut cur = Cursor::new(Vec::new());
        let name = Name::root();
        name.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(vec![0], buf);
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_write_network_bytes_not_root() {
        let mut cur = Cursor::new(Vec::new());
        let name = Name::from_str("example.com.").unwrap();
        name.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
            ],
            buf,
        );
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_read_network_bytes_no_pointer() {
        let cur = Cursor::new(vec![
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
        ]);

        let name = Name::read_network_bytes(cur).unwrap();
        assert_eq!("example.com.", name.to_string());
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_read_network_bytes_single_pointer() {
        let mut cur = Cursor::new(vec![
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            3,                                // length
            119, 119, 119,                    // "www"
            192, 0,                           // pointer to offset 0
        ]);

        cur.set_position(13);

        let name = Name::read_network_bytes(cur).unwrap();
        assert_eq!("www.example.com.", name.to_string());
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_read_network_bytes_multiple_pointer() {
        let mut cur = Cursor::new(vec![
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            3,                                // length
            119, 119, 119,                    // "www"
            192, 0,                           // pointer to offset 0
            3,                                // length
            100, 101, 118,                    // "dev"
            192, 13,                          // pointer to offset 13, "www"
        ]);

        cur.set_position(19);

        let name = Name::read_network_bytes(cur).unwrap();
        assert_eq!("dev.www.example.com.", name.to_string());
    }

    #[test]
    fn test_name_read_network_bytes_pointer_loop() {
        let cur = Cursor::new(vec![
            192, 2, // pointer to offset 2
            192, 0, // pointer to offset 0
        ]);

        let res = Name::read_network_bytes(cur);
        assert!(res.is_err());
    }
}
