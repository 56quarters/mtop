use crate::core::MtopError;
use crate::dns::core::RecordType;
use crate::dns::name::Name;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::fmt::Display;
use std::io::{Read, Seek};
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RecordData {
    A(RecordDataA),
    NS(RecordDataNS),
    CNAME(RecordDataCNAME),
    SOA(RecordDataSOA),
    TXT(RecordDataTXT),
    AAAA(RecordDataAAAA),
    SRV(RecordDataSRV),
    OPT(RecordDataUnknown),
    Unknown(RecordDataUnknown),
}

impl RecordData {
    pub fn size(&self) -> u16 {
        match self {
            Self::A(rd) => rd.size(),
            Self::NS(rd) => rd.size(),
            Self::CNAME(rd) => rd.size(),
            Self::SOA(rd) => rd.size(),
            Self::TXT(rd) => rd.size(),
            Self::AAAA(rd) => rd.size(),
            Self::SRV(rd) => rd.size(),
            Self::OPT(rd) => rd.size(),
            Self::Unknown(rd) => rd.size(),
        }
    }

    pub fn write_network_bytes<T>(&self, buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        match self {
            Self::A(rd) => rd.write_network_bytes(buf),
            Self::NS(rd) => rd.write_network_bytes(buf),
            Self::CNAME(rd) => rd.write_network_bytes(buf),
            Self::SOA(rd) => rd.write_network_bytes(buf),
            Self::TXT(rd) => rd.write_network_bytes(buf),
            Self::AAAA(rd) => rd.write_network_bytes(buf),
            Self::SRV(rd) => rd.write_network_bytes(buf),
            Self::OPT(rd) => rd.write_network_bytes(buf),
            Self::Unknown(rd) => rd.write_network_bytes(buf),
        }
    }

    pub fn read_network_bytes<T>(rtype: RecordType, rdata_len: u16, buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        match rtype {
            RecordType::A => Ok(RecordData::A(RecordDataA::read_network_bytes(buf)?)),
            RecordType::NS => Ok(RecordData::NS(RecordDataNS::read_network_bytes(buf)?)),
            RecordType::CNAME => Ok(RecordData::CNAME(RecordDataCNAME::read_network_bytes(buf)?)),
            RecordType::SOA => Ok(RecordData::SOA(RecordDataSOA::read_network_bytes(buf)?)),
            RecordType::TXT => Ok(RecordData::TXT(RecordDataTXT::read_network_bytes(rdata_len, buf)?)),
            RecordType::AAAA => Ok(RecordData::AAAA(RecordDataAAAA::read_network_bytes(buf)?)),
            RecordType::SRV => Ok(RecordData::SRV(RecordDataSRV::read_network_bytes(buf)?)),
            RecordType::OPT => Ok(RecordData::OPT(RecordDataUnknown::read_network_bytes(rdata_len, buf)?)),
            RecordType::Unknown(_) => Ok(RecordData::Unknown(RecordDataUnknown::read_network_bytes(
                rdata_len, buf,
            )?)),
        }
    }
}

impl Display for RecordData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordData::A(rd) => Display::fmt(rd, f),
            RecordData::NS(rd) => Display::fmt(rd, f),
            RecordData::CNAME(rd) => Display::fmt(rd, f),
            RecordData::SOA(rd) => Display::fmt(rd, f),
            RecordData::TXT(rd) => Display::fmt(rd, f),
            RecordData::AAAA(rd) => Display::fmt(rd, f),
            RecordData::SRV(rd) => Display::fmt(rd, f),
            RecordData::OPT(rd) => Display::fmt(rd, f),
            RecordData::Unknown(rd) => Display::fmt(rd, f),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataA(Ipv4Addr);

impl RecordDataA {
    pub fn new(addr: Ipv4Addr) -> Self {
        Self(addr)
    }

    pub fn addr(&self) -> Ipv4Addr {
        self.0
    }

    pub fn size(&self) -> u16 {
        4
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        Ok(buf.write_all(&self.0.octets())?)
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut bytes = [0_u8; 4];
        buf.read_exact(&mut bytes)?;
        Ok(RecordDataA::new(Ipv4Addr::from(bytes)))
    }
}

impl Display for RecordDataA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataNS(Name);

impl RecordDataNS {
    pub fn new(name: Name) -> Self {
        Self(name)
    }

    pub fn name(&self) -> &Name {
        &self.0
    }

    pub fn size(&self) -> u16 {
        self.0.size()
    }

    pub fn write_network_bytes<T>(&self, buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        self.0.write_network_bytes(buf)
    }

    pub fn read_network_bytes<T>(buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        Ok(Self::new(Name::read_network_bytes(buf)?))
    }
}

impl Display for RecordDataNS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataCNAME(Name);

impl RecordDataCNAME {
    pub fn new(name: Name) -> Self {
        Self(name)
    }

    pub fn name(&self) -> &Name {
        &self.0
    }

    pub fn size(&self) -> u16 {
        self.0.size()
    }

    pub fn write_network_bytes<T>(&self, buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        self.0.write_network_bytes(buf)
    }

    pub fn read_network_bytes<T>(buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        Ok(Self::new(Name::read_network_bytes(buf)?))
    }
}
impl Display for RecordDataCNAME {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataSOA {
    mname: Name,
    rname: Name,
    serial: u32,
    refresh: u32,
    retry: u32,
    expire: u32,
    minimum: u32,
}

impl RecordDataSOA {
    pub fn new(mname: Name, rname: Name, serial: u32, refresh: u32, retry: u32, expire: u32, minimum: u32) -> Self {
        Self {
            mname,
            rname,
            serial,
            refresh,
            retry,
            expire,
            minimum,
        }
    }

    pub fn mname(&self) -> &Name {
        &self.mname
    }

    pub fn rname(&self) -> &Name {
        &self.rname
    }

    pub fn serial(&self) -> u32 {
        self.serial
    }

    pub fn refresh(&self) -> u32 {
        self.refresh
    }

    pub fn retry(&self) -> u32 {
        self.retry
    }

    pub fn expire(&self) -> u32 {
        self.expire
    }

    pub fn minimum(&self) -> u32 {
        self.minimum
    }

    pub fn size(&self) -> u16 {
        self.mname.size() + self.rname.size() + (4 * 5)
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        self.mname.write_network_bytes(&mut buf)?;
        self.rname.write_network_bytes(&mut buf)?;
        buf.write_u32::<NetworkEndian>(self.serial)?;
        buf.write_u32::<NetworkEndian>(self.refresh)?;
        buf.write_u32::<NetworkEndian>(self.retry)?;
        buf.write_u32::<NetworkEndian>(self.expire)?;
        buf.write_u32::<NetworkEndian>(self.minimum)?;
        Ok(())
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mname = Name::read_network_bytes(&mut buf)?;
        let rname = Name::read_network_bytes(&mut buf)?;
        let serial = buf.read_u32::<NetworkEndian>()?;
        let refresh = buf.read_u32::<NetworkEndian>()?;
        let retry = buf.read_u32::<NetworkEndian>()?;
        let expire = buf.read_u32::<NetworkEndian>()?;
        let minimum = buf.read_u32::<NetworkEndian>()?;

        Ok(Self::new(mname, rname, serial, refresh, retry, expire, minimum))
    }
}

impl Display for RecordDataSOA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} {} {} {} {}",
            self.mname, self.rname, self.serial, self.refresh, self.retry, self.expire, self.minimum
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataTXT(Vec<Vec<u8>>);

impl RecordDataTXT {
    const MAX_LENGTH: usize = 255;
    const MAX_TOTAL_SIZE: usize = 65535;

    pub fn new<I, B>(items: I) -> Result<Self, MtopError>
    where
        I: IntoIterator<Item = B>,
        B: Into<Vec<u8>>,
    {
        let mut segments = Vec::new();
        let mut total = 0;

        for txt in items {
            let bytes = txt.into();
            if bytes.len() > Self::MAX_LENGTH {
                return Err(MtopError::runtime(format!(
                    "TXT record segment too long; {} bytes, max {} bytes",
                    bytes.len(),
                    Self::MAX_LENGTH
                )));
            }

            total += bytes.len();
            if total > Self::MAX_TOTAL_SIZE {
                return Err(MtopError::runtime(format!(
                    "TXT record too long; {} bytes, max {} bytes",
                    total,
                    Self::MAX_TOTAL_SIZE
                )));
            }

            segments.push(bytes);
        }

        Ok(Self(segments))
    }

    pub fn bytes(&self) -> &Vec<Vec<u8>> {
        &self.0
    }

    pub fn size(&self) -> u16 {
        (self.0.iter().map(|v| v.len()).sum::<usize>() + self.0.len()) as u16
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        for txt in self.0.iter() {
            buf.write_u8(txt.len() as u8)?;
            buf.write_all(txt)?;
        }

        Ok(())
    }

    pub fn read_network_bytes<T>(rdata_len: u16, mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut all = Vec::new();

        let mut consumed = 0;
        while consumed < rdata_len {
            let len = buf.read_u8()?;
            if len as u16 + consumed > rdata_len {
                return Err(MtopError::runtime(format!(
                    "text for RecordDataTXT exceeds rdata size; len: {}, consumed: {}, rdata: {}",
                    len, consumed, rdata_len
                )));
            }

            let mut txt = Vec::new();
            let mut handle = buf.take(len as u64);
            let n = handle.read_to_end(&mut txt)?;
            if n != len as usize {
                return Err(MtopError::runtime(format!(
                    "short read for RecordDataTXT text; expected {}, got {}",
                    len, n
                )));
            }

            all.push(txt);
            consumed += n as u16 + 1;
            buf = handle.into_inner();
        }

        Self::new(all)
    }
}

impl Display for RecordDataTXT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for txt in self.0.iter() {
            // We're trying to display the record so make an attempt at converting to
            // a string but don't return an error or panic if there's invalid UTF-8. We
            // also escape any double quotes within the string since we use those to
            // delimit the string.
            write!(f, "\"{}\"", String::from_utf8_lossy(txt).replace('\"', "\\\""))?
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataAAAA(Ipv6Addr);

impl RecordDataAAAA {
    pub fn new(addr: Ipv6Addr) -> Self {
        Self(addr)
    }

    pub fn addr(&self) -> Ipv6Addr {
        self.0
    }

    pub fn size(&self) -> u16 {
        16
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        Ok(buf.write_all(&self.0.octets())?)
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut bytes = [0_u8; 16];
        buf.read_exact(&mut bytes)?;
        Ok(RecordDataAAAA::new(Ipv6Addr::from(bytes)))
    }
}

impl Display for RecordDataAAAA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataSRV {
    priority: u16,
    weight: u16,
    port: u16,
    target: Name,
}

impl RecordDataSRV {
    pub fn new(priority: u16, weight: u16, port: u16, target: Name) -> Self {
        Self {
            priority,
            weight,
            port,
            target,
        }
    }

    pub fn priority(&self) -> u16 {
        self.priority
    }

    pub fn weight(&self) -> u16 {
        self.weight
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn target(&self) -> &Name {
        &self.target
    }

    pub fn size(&self) -> u16 {
        (2 * 3) + self.target.size()
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        buf.write_u16::<NetworkEndian>(self.priority)?;
        buf.write_u16::<NetworkEndian>(self.weight)?;
        buf.write_u16::<NetworkEndian>(self.port)?;
        self.target.write_network_bytes(buf)
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let priority = buf.read_u16::<NetworkEndian>()?;
        let weight = buf.read_u16::<NetworkEndian>()?;
        let port = buf.read_u16::<NetworkEndian>()?;
        let target = Name::read_network_bytes(buf)?;

        Ok(Self::new(priority, weight, port, target))
    }
}

impl Display for RecordDataSRV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {} {}", self.priority, self.weight, self.port, self.target)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordDataUnknown(Vec<u8>);

impl RecordDataUnknown {
    const MAX_SIZE: usize = 65535;

    pub fn new(bytes: Vec<u8>) -> Result<Self, MtopError> {
        if bytes.len() > Self::MAX_SIZE {
            Err(MtopError::runtime(format!(
                "record data too long; {} bytes, max {} bytes",
                bytes.len(),
                Self::MAX_SIZE
            )))
        } else {
            Ok(Self(bytes))
        }
    }

    pub fn size(&self) -> u16 {
        self.0.len() as u16
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        buf.write_all(&self.0)?;
        Ok(())
    }

    pub fn read_network_bytes<T>(rdata_len: u16, buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let mut bytes = Vec::new();
        let n = buf.take(rdata_len as u64).read_to_end(&mut bytes)?;
        if n != rdata_len as usize {
            return Err(MtopError::runtime(format!(
                "short read for RecordDataUnknown; expected {} got {}",
                rdata_len, n
            )));
        }

        Self::new(bytes)
    }
}

impl Display for RecordDataUnknown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[unknown]")
    }
}

#[cfg(test)]
mod test {
    use super::{
        RecordDataA, RecordDataAAAA, RecordDataCNAME, RecordDataNS, RecordDataSOA, RecordDataSRV, RecordDataTXT,
    };
    use crate::dns::name::Name;
    use std::io::Cursor;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::str::FromStr;

    #[test]
    fn test_record_data_a_write_network_bytes() {
        let rdata = RecordDataA::new(Ipv4Addr::new(127, 0, 0, 1));

        let mut cur = Cursor::new(Vec::new());
        rdata.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(vec![127, 0, 0, 1], buf);
    }
    #[test]
    fn test_record_data_a_read_network_bytes() {
        let cur = Cursor::new(vec![127, 0, 0, 53]);
        let rdata = RecordDataA::read_network_bytes(cur).unwrap();

        assert_eq!(Ipv4Addr::new(127, 0, 0, 53), rdata.addr());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_ns_write_network_bytes() {
        let name = Name::from_str("ns.example.com.").unwrap();
        let ns = RecordDataNS::new(name);

        let mut cur = Cursor::new(Vec::new());
        ns.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                2,                                // length
                110, 115,                         // "ns"
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
    fn test_record_data_ns_read_network_bytes() {
        let cur = Cursor::new(vec![
            2,                                // length
            110, 115,                         // "ns"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
        ]);

        let rdata = RecordDataNS::read_network_bytes(cur).unwrap();
        assert_eq!("ns.example.com.", rdata.name().to_string());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_cname_write_network_bytes() {
        let name = Name::from_str("www.example.com.").unwrap();
        let ns = RecordDataCNAME::new(name);

        let mut cur = Cursor::new(Vec::new());
        ns.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                3,                                // length
                119, 119, 119,                    // "www"
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
    fn test_record_data_cname_read_network_bytes() {
        let cur = Cursor::new(vec![
            3,                                // length
            119, 119, 119,                    // "www"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
        ]);

        let rdata = RecordDataCNAME::read_network_bytes(cur).unwrap();
        assert_eq!("www.example.com.", rdata.name().to_string());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_soa_write_network_bytes() {
        let mname = Name::from_str("m.example.com.").unwrap();
        let rname = Name::from_str("r.example.com.").unwrap();
        let serial = 123456790;
        let refresh = 3000;
        let retry = 300;
        let expire = 3600;
        let minimum = 600;

        let soa = RecordDataSOA::new(mname, rname, serial, refresh, retry, expire, minimum);
        let mut cur = Cursor::new(Vec::new());
        soa.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
             vec![
                1,                                // length
                109,                              // "m"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
                1,                                // length
                114,                              // "r"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
                7, 91, 205, 22,                   // serial
                0, 0, 11, 184,                    // refresh
                0, 0, 1, 44,                      // retry
                0, 0, 14, 16,                     // expire
                0, 0, 2, 88                       // minimum
             ],
             buf,
         );
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_soa_read_network_bytes() {
        let cur = Cursor::new(vec![
            1,                                // length
            109,                              // "m"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            1,                                // length
            114,                              // "r"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            7, 91, 205, 22,                   // serial
            0, 0, 11, 184,                    // refresh
            0, 0, 1, 44,                      // retry
            0, 0, 14, 16,                     // expire
            0, 0, 2, 88                       // minimum
        ]);

        let rdata = RecordDataSOA::read_network_bytes(cur).unwrap();
        assert_eq!("m.example.com.", rdata.mname().to_string());
        assert_eq!("r.example.com.", rdata.rname().to_string());
        assert_eq!(123456790, rdata.serial());
        assert_eq!(3000, rdata.refresh());
        assert_eq!(300, rdata.retry());
        assert_eq!(3600, rdata.expire());
        assert_eq!(600, rdata.minimum());
    }

    #[test]
    fn test_record_data_txt_size() {
        let txt = RecordDataTXT::new(vec!["id=hello", "user=world"]).unwrap();
        assert_eq!(20, txt.size());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_txt_write_network_bytes() {
        let txt = RecordDataTXT::new(vec!["id=hello", "user=world"]).unwrap();
        let mut cur = Cursor::new(Vec::new());
        txt.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                8,                                               // length
                105, 100, 61, 104, 101, 108, 108, 111,           // id=hello
                10,                                              // length
                117, 115, 101, 114, 61, 119, 111, 114, 108, 100, // user=world
            ],
            buf,
        )
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_txt_read_network_bytes() {
        let bytes = vec![
            8,                                               // length
            105, 100, 61, 104, 101, 108, 108, 111,           // id=hello
            10,                                              // length
            117, 115, 101, 114, 61, 119, 111, 114, 108, 100, // user=world
        ];
        let bytes_len = bytes.len() as u16;
        let cur = Cursor::new(bytes);

        let rdata = RecordDataTXT::read_network_bytes(bytes_len, cur).unwrap();
        let contents = rdata.bytes();
        assert_eq!("id=hello".as_bytes(), contents[0]);
        assert_eq!("user=world".as_bytes(), contents[1]);
    }

    #[test]
    fn test_record_data_aaaa_write_network_bytes() {
        let addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1);
        let rdata = RecordDataAAAA::new(addr);

        let mut cur = Cursor::new(Vec::new());
        rdata.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], buf);
    }

    #[test]
    fn test_record_data_aaaa_read_network_bytes() {
        let cur = Cursor::new(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        let rdata = RecordDataAAAA::read_network_bytes(cur).unwrap();

        assert_eq!(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), rdata.addr());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_srv_write_network_bytes() {
        let srv = RecordDataSRV::new(100, 20, 11211, Name::from_str("_cache.example.com.").unwrap());
        let mut cur = Cursor::new(Vec::new());
        srv.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                0, 100,                           // priority
                0, 20,                            // weight
                43, 203,                          // port
                6,                                // length
                95, 99, 97, 99, 104, 101,         // "_cache"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
            ],
            buf,
        )
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_data_srv_read_network_bytes() {
        let cur = Cursor::new(vec![
            0, 100,                           // priority
            0, 20,                            // weight
            43, 203,                          // port
            6,                                // length
            95, 99, 97, 99, 104, 101,         // "_cache"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
        ]);

        let rdata = RecordDataSRV::read_network_bytes(cur).unwrap();
        assert_eq!(100, rdata.priority());
        assert_eq!(20, rdata.weight());
        assert_eq!(11211, rdata.port());
        assert_eq!("_cache.example.com.", rdata.target().to_string());
    }
}
