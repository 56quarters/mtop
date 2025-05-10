use crate::core::MtopError;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Name {
    labels: Vec<Vec<u8>>,
    is_fqdn: bool,
}

impl Name {
    const MAX_LENGTH: usize = 255;
    const MAX_LABEL_LENGTH: usize = 63;
    const MAX_POINTERS: u32 = 64;

    pub fn root() -> Self {
        Name {
            labels: Vec::new(),
            is_fqdn: true,
        }
    }

    pub fn size(&self) -> usize {
        self.labels.iter().map(|l| l.len()).sum::<usize>() + self.labels.len() + 1
    }

    pub fn is_root(&self) -> bool {
        self.labels.is_empty() && self.is_fqdn
    }

    pub fn is_fqdn(&self) -> bool {
        self.is_fqdn
    }

    pub fn to_fqdn(mut self) -> Self {
        self.is_fqdn = true;
        self
    }

    pub fn append(mut self, other: Name) -> Self {
        if self.is_fqdn {
            return self;
        }

        self.labels.extend(other.labels);
        Self {
            labels: self.labels,
            is_fqdn: other.is_fqdn,
        }
    }

    pub fn write_network_bytes<T>(&self, mut out: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        // We convert all incoming Names to fully qualified names. If we missed doing
        // that, it's a bug and we should panic here. Encoded names all end with the
        // root so trying to encode something that doesn't makes no sense.
        assert!(self.is_fqdn, "only fully qualified domains can be encoded");

        for label in self.labels.iter() {
            out.write_u8(label.len() as u8)?;
            out.write_all(label)?;
        }

        Ok(out.write_u8(0)?)
    }

    pub fn read_network_bytes<T>(mut inp: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut name = Vec::new();
        Self::read_inner(&mut inp, &mut name)?;
        Self::from_bytes(&name)
    }

    /// Read the DNS message format bytes for a `Name` and write them to `out`,
    /// following any pointers (how names are compressed in DNS messages). The
    /// bytes written to `out` are ASCII characters of the text representation
    /// of the name, e.g. `"example.com.".as_bytes()`.
    fn read_inner<T>(inp: &mut T, out: &mut Vec<u8>) -> Result<(), MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut pointers = 0;
        let mut position = None;
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

            let len = inp.read_u8()?;
            // If the length isn't a length but actually a pointer to another name
            // or label within the message, seek to that position within the message
            // and read the name from there. After resolving all pointers and reading
            // labels, reset the stream back to immediately after the first pointer.
            if Self::is_compressed_label(len) {
                let offset = Self::get_offset(len, inp.read_u8()?);
                if position.is_none() {
                    position = Some(inp.stream_position()?);
                }

                inp.seek(SeekFrom::Start(u64::from(offset)))?;
                pointers += 1;
            } else if Self::is_standard_label(len) {
                // If the length is a length, read the next label (segment) of the name
                // returning early once we read the "root" label (`.`) signified by a
                // length of 0.
                if Self::read_label_into(inp, len, out)? {
                    if let Some(p) = position {
                        // If we followed a pointer to different part of the message while
                        // parsing this name, seek to the position immediately after the
                        // pointer now that we've finished parsing this name.
                        inp.seek(SeekFrom::Start(p))?;
                    }

                    return Ok(());
                }
            } else {
                // Binary labels are deprecated (RFC 6891) and there are (currently) no other
                // types of labels that we should expect. Return an error to make this obvious.
                return Err(MtopError::runtime(format!("unsupported Name label type: {}", len)));
            }
        }
    }

    /// If `len` doesn't indicate this is the root label, read the next name label into
    /// `out` followed by a `.` and return false, true if next label was the root.
    fn read_label_into<T>(inp: &mut T, len: u8, out: &mut Vec<u8>) -> Result<bool, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        if len == 0 {
            return Ok(true);
        }

        // Only six bits of the length are supposed to be used to encode the
        // length of a label so 63 is the max length but double check just in
        // case one of the pointer bits was set for some reason.
        if usize::from(len) > Self::MAX_LABEL_LENGTH {
            return Err(MtopError::runtime(format!(
                "max size for label would be exceeded reading {} bytes",
                len,
            )));
        }

        if usize::from(len) + out.len() + 1 > Self::MAX_LENGTH {
            return Err(MtopError::runtime(format!(
                "max size for name would be exceeded adding {} bytes to {}",
                len,
                out.len()
            )));
        }

        let mut handle = inp.take(u64::from(len));
        let n = handle.read_to_end(out)?;
        if n != usize::from(len) {
            return Err(MtopError::runtime(format!(
                "short read for Name segment. expected {} got {}",
                len, n
            )));
        }

        out.push(b'.');
        Ok(false)
    }

    fn is_standard_label(len: u8) -> bool {
        len & 0b1100_0000 == 0
    }

    fn is_compressed_label(len: u8) -> bool {
        // The top two bits of the length byte of a name label (section) are used
        // to indicate the name is actually an offset in the DNS message to a previous
        // name to avoid duplicating the same names over and over.
        len & 0b1100_0000 == 192
    }

    fn get_offset(len: u8, next: u8) -> u16 {
        let pointer = u16::from(len & 0b0011_1111) << 8;
        pointer | u16::from(next)
    }

    /// Construct a new `Name` from the bytes of a string representation of a domain
    /// name, e.g. `"example.com.".as_bytes()`. This method validates the total length
    /// of the name, the length of each label, and the characters used for each label.
    /// An empty byte slice or a byte slice with only the ASCII `.` character are treated
    /// as the root domain.
    fn from_bytes(bytes: &[u8]) -> Result<Self, MtopError> {
        if bytes.is_empty() || bytes == b"." {
            return Ok(Self::root());
        }

        if bytes.len() > Self::MAX_LENGTH {
            return Err(MtopError::configuration(format!(
                "Name too long; max {} bytes, got {}",
                Self::MAX_LENGTH,
                bytes.len()
            )));
        }

        // Trim any trailing dot from the domain which indicates that it is fully qualified
        // and make a note of it. This is required since we're splitting the domain into each
        // individual label and need to know how to construct the correct string form afterward.
        let (bytes, is_fqdn) = match bytes.strip_suffix(b".") {
            Some(stripped) => (stripped, true),
            None => (bytes, false),
        };

        // Start with space for 4 labels which covers the size of typical domains.
        let mut labels = Vec::with_capacity(4);

        for label in bytes.split(|&b| b == b'.') {
            let label_len = label.len();

            for (i, b) in label.iter().enumerate() {
                let c = char::from(*b);
                if i + 1 > Self::MAX_LABEL_LENGTH {
                    return Err(MtopError::configuration(format!(
                        "label too long; max {} bytes, got {}",
                        Self::MAX_LABEL_LENGTH,
                        i + 1,
                    )));
                } else if i == 0 && c != '_' && !c.is_ascii_alphanumeric() {
                    return Err(MtopError::configuration(format!(
                        "label must begin with ASCII letter, number, or underscore; got {}",
                        c
                    )));
                } else if i == label_len - 1 && !c.is_ascii_alphanumeric() {
                    return Err(MtopError::configuration(format!(
                        "label must end with ASCII letter or number; got {}",
                        c
                    )));
                } else if c != '-' && c != '_' && !c.is_ascii_alphanumeric() {
                    return Err(MtopError::configuration(format!(
                        "label must be ASCII letter, number, hyphen, or underscore; got {}",
                        c
                    )));
                }
            }

            labels.push(label.to_ascii_lowercase());
        }

        Ok(Self { labels, is_fqdn })
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.labels.len();
        for (i, l) in self.labels.iter().enumerate() {
            // Labels are all valid ASCII so there aren't any replacement characters
            // needed. We just want to create a string from a bytes slice and not
            // allocate. This is the easiest way that doesn't involve unsafe methods.
            String::from_utf8_lossy(l).fmt(f)?;
            if i != len - 1 {
                ".".fmt(f)?;
            }
        }
        if self.is_fqdn {
            ".".fmt(f)?;
        }

        Ok(())
    }
}

impl FromStr for Name {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(s.as_bytes())
    }
}

#[cfg(test)]
mod test {
    use super::Name;
    use std::io::Cursor;
    use std::str::FromStr;

    #[test]
    fn test_name_from_str_max_length() {
        let parts = [
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
        let parts = ["a".repeat(Name::MAX_LABEL_LENGTH + 1), "com.".to_owned()];
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
    fn test_name_from_str_success_not_fqdn() {
        let name = Name::from_str("example.com").unwrap();
        assert!(!name.is_root());
        assert!(!name.is_fqdn());
    }

    #[test]
    fn test_name_from_str_success_fqdn() {
        let name = Name::from_str("example.com.").unwrap();
        assert!(!name.is_root());
        assert!(name.is_fqdn());
    }

    #[test]
    fn test_name_from_str_success_root_empty() {
        let name = Name::from_str("").unwrap();
        assert!(name.is_root());
        assert!(name.is_fqdn());
    }

    #[test]
    fn test_name_from_str_success_root_dot() {
        let name = Name::from_str(".").unwrap();
        assert!(name.is_root());
        assert!(name.is_fqdn());
    }

    #[test]
    fn test_name_to_string_not_fqdn() {
        let name = Name::from_str("example.com").unwrap();
        assert_eq!("example.com", name.to_string());
        assert!(!name.is_fqdn());
    }

    #[test]
    fn test_name_to_string_fqdn() {
        let name = Name::from_str("example.com.").unwrap();
        assert_eq!("example.com.", name.to_string());
        assert!(name.is_fqdn());
    }

    #[test]
    fn test_name_to_string_root() {
        let name = Name::root();
        assert_eq!(".", name.to_string());
        assert!(name.is_fqdn());
    }

    #[test]
    fn test_name_to_fqdn_not_fqdn() {
        let name = Name::from_str("example.com").unwrap();
        assert!(!name.is_fqdn());

        let fqdn = name.to_fqdn();
        assert!(fqdn.is_fqdn());
    }

    #[test]
    fn test_name_to_fqdn_already_fqdn() {
        let name = Name::from_str("example.com.").unwrap();
        assert!(name.is_fqdn());

        let fqdn = name.to_fqdn();
        assert!(fqdn.is_fqdn());
    }

    #[test]
    fn test_name_append_already_fqdn() {
        let name1 = Name::from_str("example.com.").unwrap();
        let name2 = Name::from_str("example.net.").unwrap();
        let combined = name1.clone().append(name2);

        assert_eq!(name1, combined);
        assert!(combined.is_fqdn());
    }

    #[test]
    fn test_name_append_with_non_fqdn() {
        let name1 = Name::from_str("www").unwrap();
        let name2 = Name::from_str("example").unwrap();
        let combined = name1.clone().append(name2);

        assert_eq!(Name::from_str("www.example").unwrap(), combined);
        assert!(!combined.is_fqdn());
    }

    #[test]
    fn test_name_append_with_fqdn() {
        let name1 = Name::from_str("www").unwrap();
        let name2 = Name::from_str("example.net.").unwrap();
        let combined = name1.clone().append(name2);

        assert_eq!(Name::from_str("www.example.net.").unwrap(), combined);
        assert!(combined.is_fqdn());
    }

    #[test]
    fn test_name_append_with_root() {
        let name = Name::from_str("example.com").unwrap();
        let combined = name.clone().append(Name::root());

        assert_eq!(Name::from_str("example.com.").unwrap(), combined);
        assert!(combined.is_fqdn());
    }

    #[test]
    fn test_name_append_multiple() {
        let name1 = Name::from_str("dev").unwrap();
        let name2 = Name::from_str("www").unwrap();
        let name3 = Name::from_str("example.com").unwrap();

        let combined = name1.append(name2).append(name3).append(Name::root());
        assert_eq!(Name::from_str("dev.www.example.com.").unwrap(), combined);
        assert!(combined.is_fqdn());
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
    fn test_name_size_non_root_fqdn() {
        let name = Name::from_str("example.com").unwrap();
        assert!(!name.is_fqdn());
        assert_eq!(13, name.size());

        // The purpose of the .size() method is to figure out how many bytes this
        // name would be when serialized to binary message format. Only FQDN can be
        // serialized so we expect the size to be the same between the non-FQDN and
        // FQDN version of this name.
        let name = name.to_fqdn();
        assert!(name.is_fqdn());
        assert_eq!(13, name.size());
    }

    #[test]
    fn test_name_equal_same_case() {
        let name1 = Name::from_str("example.com.").unwrap();
        let name2 = Name::from_str("example.com.").unwrap();

        assert_eq!(name1, name2);
    }

    #[test]
    fn test_name_equal_different_case() {
        let name1 = Name::from_str("example.com.").unwrap();
        let name2 = Name::from_str("EXAMPLE.cOm.").unwrap();

        assert_eq!(name1, name2);
    }

    #[test]
    fn test_name_equal_different_fqdn() {
        let name1 = Name::from_str("example.com").unwrap();
        let name2 = Name::from_str("example.com.").unwrap();

        assert_ne!(name1, name2);
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

    #[should_panic]
    #[test]
    fn test_name_write_network_bytes_not_fqdn() {
        let mut cur = Cursor::new(Vec::new());
        let name = Name::from_str("example.com").unwrap();
        let _ = name.write_network_bytes(&mut cur);
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
        assert!(name.is_fqdn());
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_read_network_bytes_bad_label_type() {
        let cur = Cursor::new(vec![
            64, // length, deprecated binary labels from RFC 2673
            0,  // count of binary labels
        ]);

        let res = Name::read_network_bytes(cur);
        assert!(res.is_err());
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_read_network_bytes_bad_label_type_after_single_pointer() {
        let mut cur = Cursor::new(vec![
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            64,                               // length, deprecated binary labels from RFC 2673
            0,                                // count of binary labels
            3,                                // length
            119, 119, 119,                    // "www"
            192, 0,                           // pointer to offset 0
        ]);

        cur.set_position(10);

        let res = Name::read_network_bytes(&mut cur);
        assert!(res.is_err());
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

        let name = Name::read_network_bytes(&mut cur).unwrap();
        assert_eq!("www.example.com.", name.to_string());
        assert!(name.is_fqdn());
        assert_eq!(19, cur.position());
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

        let name = Name::read_network_bytes(&mut cur).unwrap();
        assert_eq!("dev.www.example.com.", name.to_string());
        assert!(name.is_fqdn());
        assert_eq!(25, cur.position());
    }

    #[rustfmt::skip]
    #[test]
    fn test_name_read_network_bytes_multiple_pointer_multiple_name() {
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

        let name1 = Name::read_network_bytes(&mut cur).unwrap();
        assert_eq!("example.com.", name1.to_string());
        assert!(name1.is_fqdn());

        let name2 = Name::read_network_bytes(&mut cur).unwrap();
        assert_eq!("www.example.com.", name2.to_string());
        assert!(name2.is_fqdn());

        let name3 = Name::read_network_bytes(&mut cur).unwrap();
        assert_eq!("dev.www.example.com.", name3.to_string());
        assert!(name3.is_fqdn());

        assert_eq!(25, cur.position());
    }

    #[test]
    fn test_name_read_network_bytes_pointer_loop() {
        let mut cur = Cursor::new(vec![
            192, 2, // pointer to offset 2
            192, 0, // pointer to offset 0
        ]);

        let res = Name::read_network_bytes(&mut cur);
        assert!(res.is_err());
    }
}
