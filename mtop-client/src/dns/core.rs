use crate::core::MtopError;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u16)]
pub enum RecordType {
    A,
    NS,
    CNAME,
    SOA,
    TXT,
    AAAA,
    SRV,
    OPT,
    Unknown(u16),
}

impl From<u16> for RecordType {
    fn from(value: u16) -> Self {
        match value {
            1 => Self::A,
            2 => Self::NS,
            5 => Self::CNAME,
            6 => Self::SOA,
            16 => Self::TXT,
            28 => Self::AAAA,
            33 => Self::SRV,
            41 => Self::OPT,
            v => Self::Unknown(v),
        }
    }
}

impl From<RecordType> for u16 {
    fn from(value: RecordType) -> Self {
        match value {
            RecordType::A => 1,
            RecordType::NS => 2,
            RecordType::CNAME => 5,
            RecordType::SOA => 6,
            RecordType::TXT => 16,
            RecordType::AAAA => 28,
            RecordType::SRV => 33,
            RecordType::OPT => 41,
            RecordType::Unknown(c) => c,
        }
    }
}

impl Display for RecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordType::A => write!(f, "A"),
            RecordType::NS => write!(f, "NS"),
            RecordType::CNAME => write!(f, "CNAME"),
            RecordType::SOA => write!(f, "SOA"),
            RecordType::TXT => write!(f, "TXT"),
            RecordType::AAAA => write!(f, "AAAA"),
            RecordType::SRV => write!(f, "SRV"),
            RecordType::OPT => write!(f, "OPT"),
            RecordType::Unknown(t) => write!(f, "Unknown({})", t),
        }
    }
}

impl FromStr for RecordType {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();
        match s.as_ref() {
            "A" => Ok(RecordType::A),
            "NS" => Ok(RecordType::NS),
            "CNAME" => Ok(RecordType::CNAME),
            "SOA" => Ok(RecordType::SOA),
            "TXT" => Ok(RecordType::TXT),
            "AAAA" => Ok(RecordType::AAAA),
            "SRV" => Ok(RecordType::SRV),
            "OPT" => Ok(RecordType::OPT),
            v => Err(MtopError::configuration(format!("unknown record type '{}'", v))),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u16)]
pub enum RecordClass {
    INET,
    CHAOS,
    HESIOD,
    NONE,
    ANY,
    Unknown(u16),
}

impl From<u16> for RecordClass {
    fn from(value: u16) -> Self {
        match value {
            1 => Self::INET,
            3 => Self::CHAOS,
            4 => Self::HESIOD,
            254 => Self::NONE,
            255 => Self::ANY,
            v => Self::Unknown(v),
        }
    }
}

impl From<RecordClass> for u16 {
    fn from(value: RecordClass) -> Self {
        match value {
            RecordClass::INET => 1,
            RecordClass::CHAOS => 3,
            RecordClass::HESIOD => 4,
            RecordClass::NONE => 254,
            RecordClass::ANY => 255,
            RecordClass::Unknown(c) => c,
        }
    }
}

impl Display for RecordClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordClass::INET => write!(f, "INET"),
            RecordClass::HESIOD => write!(f, "HESIOD"),
            RecordClass::CHAOS => write!(f, "CHAOS"),
            RecordClass::NONE => write!(f, "NONE"),
            RecordClass::ANY => write!(f, "ANY"),
            RecordClass::Unknown(c) => write!(f, "Unknown({})", c),
        }
    }
}

impl FromStr for RecordClass {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();
        match s.as_ref() {
            "INET" => Ok(RecordClass::INET),
            "HESIOD" => Ok(RecordClass::HESIOD),
            "CHAOS" => Ok(RecordClass::CHAOS),
            "NONE" => Ok(RecordClass::NONE),
            "ANY" => Ok(RecordClass::ANY),
            v => Err(MtopError::configuration(format!("unknown record class '{}'", v))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{RecordClass, RecordType};
    use std::str::FromStr;

    #[test]
    fn test_record_type_from_u16() {
        assert_eq!(RecordType::A, RecordType::from(1));
        assert_eq!(RecordType::NS, RecordType::from(2));
        assert_eq!(RecordType::CNAME, RecordType::from(5));
        assert_eq!(RecordType::SOA, RecordType::from(6));
        assert_eq!(RecordType::TXT, RecordType::from(16));
        assert_eq!(RecordType::AAAA, RecordType::from(28));
        assert_eq!(RecordType::SRV, RecordType::from(33));
        assert_eq!(RecordType::OPT, RecordType::from(41));
        assert_eq!(RecordType::Unknown(999), RecordType::from(999));
    }

    #[test]
    fn test_record_type_to_u16() {
        assert_eq!(1_u16, RecordType::A.into());
        assert_eq!(2_u16, RecordType::NS.into());
        assert_eq!(5_u16, RecordType::CNAME.into());
        assert_eq!(6_u16, RecordType::SOA.into());
        assert_eq!(16_u16, RecordType::TXT.into());
        assert_eq!(28_u16, RecordType::AAAA.into());
        assert_eq!(33_u16, RecordType::SRV.into());
        assert_eq!(41_u16, RecordType::OPT.into());
        assert_eq!(999_u16, RecordType::Unknown(999).into());
    }

    #[test]
    fn test_record_type_display() {
        assert_eq!("A", RecordType::A.to_string());
        assert_eq!("NS", RecordType::NS.to_string());
        assert_eq!("CNAME", RecordType::CNAME.to_string());
        assert_eq!("SOA", RecordType::SOA.to_string());
        assert_eq!("TXT", RecordType::TXT.to_string());
        assert_eq!("AAAA", RecordType::AAAA.to_string());
        assert_eq!("SRV", RecordType::SRV.to_string());
        assert_eq!("OPT", RecordType::OPT.to_string());
        assert_eq!("Unknown(999)", RecordType::Unknown(999).to_string());
    }

    #[test]
    fn test_record_type_from_str() {
        assert_eq!(RecordType::A, RecordType::from_str("A").unwrap());
        assert_eq!(RecordType::NS, RecordType::from_str("NS").unwrap());
        assert_eq!(RecordType::CNAME, RecordType::from_str("CNAME").unwrap());
        assert_eq!(RecordType::SOA, RecordType::from_str("SOA").unwrap());
        assert_eq!(RecordType::TXT, RecordType::from_str("TXT").unwrap());
        assert_eq!(RecordType::AAAA, RecordType::from_str("AAAA").unwrap());
        assert_eq!(RecordType::SRV, RecordType::from_str("SRV").unwrap());
        assert_eq!(RecordType::OPT, RecordType::from_str("OPT").unwrap());
        assert!(RecordType::from_str("BOGUS").is_err());
    }

    #[test]
    fn test_record_class_from_u16() {
        assert_eq!(RecordClass::INET, RecordClass::from(1));
        assert_eq!(RecordClass::CHAOS, RecordClass::from(3));
        assert_eq!(RecordClass::HESIOD, RecordClass::from(4));
        assert_eq!(RecordClass::NONE, RecordClass::from(254));
        assert_eq!(RecordClass::ANY, RecordClass::from(255));
        assert_eq!(RecordClass::Unknown(512), RecordClass::from(512));
    }

    #[test]
    fn test_record_class_to_u16() {
        assert_eq!(1_u16, RecordClass::INET.into());
        assert_eq!(3_u16, RecordClass::CHAOS.into());
        assert_eq!(4_u16, RecordClass::HESIOD.into());
        assert_eq!(254_u16, RecordClass::NONE.into());
        assert_eq!(255_u16, RecordClass::ANY.into());
        assert_eq!(512_u16, RecordClass::Unknown(512).into());
    }

    #[test]
    fn test_record_class_display() {
        assert_eq!("INET", RecordClass::INET.to_string());
        assert_eq!("CHAOS", RecordClass::CHAOS.to_string());
        assert_eq!("HESIOD", RecordClass::HESIOD.to_string());
        assert_eq!("NONE", RecordClass::NONE.to_string());
        assert_eq!("ANY", RecordClass::ANY.to_string());
        assert_eq!("Unknown(512)", RecordClass::Unknown(512).to_string());
    }

    #[test]
    fn test_record_class_from_str() {
        assert_eq!(RecordClass::INET, RecordClass::from_str("INET").unwrap());
        assert_eq!(RecordClass::CHAOS, RecordClass::from_str("CHAOS").unwrap());
        assert_eq!(RecordClass::HESIOD, RecordClass::from_str("HESIOD").unwrap());
        assert_eq!(RecordClass::NONE, RecordClass::from_str("NONE").unwrap());
        assert_eq!(RecordClass::ANY, RecordClass::from_str("ANY").unwrap());
        assert!(RecordClass::from_str("BOGUS").is_err());
    }
}
