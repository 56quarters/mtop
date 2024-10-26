use crate::core::MtopError;
use crate::dns::core::{RecordClass, RecordType};
use crate::dns::name::Name;
use crate::dns::rdata::RecordData;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::Seek;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct MessageId(u16);

impl MessageId {
    pub fn random() -> Self {
        Self(rand::random())
    }

    pub fn size(&self) -> usize {
        2
    }
}

impl From<u16> for MessageId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<MessageId> for u16 {
    fn from(value: MessageId) -> Self {
        value.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Message {
    id: MessageId,
    flags: Flags,
    questions: Vec<Question>,
    answers: Vec<Record>,
    authority: Vec<Record>,
    extra: Vec<Record>,
}

impl Message {
    pub fn new(id: MessageId, flags: Flags) -> Self {
        Self {
            id,
            flags,
            questions: Vec::new(),
            answers: Vec::new(),
            authority: Vec::new(),
            extra: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.id.size()
            + self.flags.size()
            + (2 * 4) // lengths of questions, answers, authority, extra
            + self.questions.iter().map(|q| q.size()).sum::<usize>()
            + self.answers.iter().map(|r| r.size()).sum::<usize>()
            + self.authority.iter().map(|r| r.size()).sum::<usize>()
            + self.extra.iter().map(|r| r.size()).sum::<usize>()
    }

    pub fn id(&self) -> MessageId {
        self.id
    }

    pub fn flags(&self) -> Flags {
        self.flags
    }

    pub fn questions(&self) -> &[Question] {
        &self.questions
    }

    pub fn add_question(mut self, q: Question) -> Self {
        self.questions.push(q);
        self
    }

    pub fn answers(&self) -> &[Record] {
        &self.answers
    }

    pub fn add_answer(mut self, r: Record) -> Self {
        self.answers.push(r);
        self
    }

    pub fn authority(&self) -> &[Record] {
        &self.authority
    }

    pub fn add_authority(mut self, r: Record) -> Self {
        self.authority.push(r);
        self
    }

    pub fn extra(&self) -> &[Record] {
        &self.extra
    }

    pub fn add_extra(mut self, r: Record) -> Self {
        self.extra.push(r);
        self
    }

    fn header(&self) -> Header {
        assert!(self.questions.len() < usize::from(u16::MAX));
        assert!(self.answers.len() < usize::from(u16::MAX));
        assert!(self.authority.len() < usize::from(u16::MAX));
        assert!(self.extra.len() < usize::from(u16::MAX));

        Header {
            id: self.id,
            flags: self.flags,
            num_questions: self.questions.len() as u16,
            num_answers: self.answers.len() as u16,
            num_authority: self.authority.len() as u16,
            num_extra: self.extra.len() as u16,
        }
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        let header = self.header();
        header.write_network_bytes(&mut buf)?;

        for q in self.questions.iter() {
            q.write_network_bytes(&mut buf)?;
        }

        for r in self.answers.iter() {
            r.write_network_bytes(&mut buf)?;
        }

        for r in self.authority.iter() {
            r.write_network_bytes(&mut buf)?;
        }

        for r in self.extra.iter() {
            r.write_network_bytes(&mut buf)?;
        }

        Ok(())
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let header = Header::read_network_bytes(&mut buf)?;

        let mut questions = Vec::new();
        for _ in 0..header.num_questions {
            questions.push(Question::read_network_bytes(&mut buf)?);
        }

        let mut answers = Vec::new();
        for _ in 0..header.num_answers {
            answers.push(Record::read_network_bytes(&mut buf)?);
        }

        let mut authority = Vec::new();
        for _ in 0..header.num_authority {
            authority.push(Record::read_network_bytes(&mut buf)?);
        }

        let mut extra = Vec::new();
        for _ in 0..header.num_extra {
            extra.push(Record::read_network_bytes(&mut buf)?);
        }

        Ok(Self {
            id: header.id,
            flags: header.flags,
            questions,
            answers,
            authority,
            extra,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Header {
    id: MessageId,
    flags: Flags,
    num_questions: u16,
    num_answers: u16,
    num_authority: u16,
    num_extra: u16,
}

impl Header {
    fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        buf.write_u16::<NetworkEndian>(self.id.into())?;
        buf.write_u16::<NetworkEndian>(self.flags.as_u16())?;
        buf.write_u16::<NetworkEndian>(self.num_questions)?;
        buf.write_u16::<NetworkEndian>(self.num_answers)?;
        buf.write_u16::<NetworkEndian>(self.num_authority)?;
        Ok(buf.write_u16::<NetworkEndian>(self.num_extra)?)
    }

    fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt,
    {
        let id = MessageId::from(buf.read_u16::<NetworkEndian>()?);
        let flags = Flags::try_from(buf.read_u16::<NetworkEndian>()?)?;
        let num_questions = buf.read_u16::<NetworkEndian>()?;
        let num_answers = buf.read_u16::<NetworkEndian>()?;
        let num_authority = buf.read_u16::<NetworkEndian>()?;
        let num_extra = buf.read_u16::<NetworkEndian>()?;

        Ok(Header {
            id,
            flags,
            num_questions,
            num_answers,
            num_authority,
            num_extra,
        })
    }
}

#[derive(Default, Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
pub struct Flags(u16);

impl Flags {
    const MASK_QR: u16 = 0b1000_0000_0000_0000; // query / response
    const MASK_OP: u16 = 0b0111_1000_0000_0000; // 4 bits, op code
    const MASK_AA: u16 = 0b0000_0100_0000_0000; // authoritative answer
    const MASK_TC: u16 = 0b0000_0010_0000_0000; // truncated
    const MASK_RD: u16 = 0b0000_0001_0000_0000; // recursion desired
    const MASK_RA: u16 = 0b0000_0000_1000_0000; // recursion available
    const MASK_RC: u16 = 0b0000_0000_0000_1111; // 4 bits, response code

    const OFFSET_QR: usize = 15;
    const OFFSET_OP: usize = 11;
    const OFFSET_AA: usize = 10;
    const OFFSET_TC: usize = 9;
    const OFFSET_RD: usize = 8;
    const OFFSET_RA: usize = 7;
    const OFFSET_RC: usize = 0;

    pub fn size(&self) -> usize {
        2
    }

    pub fn is_query(&self) -> bool {
        !(self.0 & Self::MASK_QR) > 0
    }

    pub fn set_query(self) -> Self {
        Flags(self.0 & !Self::MASK_QR)
    }

    pub fn is_response(&self) -> bool {
        self.0 & Self::MASK_QR > 0
    }

    pub fn set_response(self) -> Self {
        Flags(self.0 | Self::MASK_QR)
    }

    pub fn get_op_code(&self) -> Operation {
        Operation::try_from((self.0 & Self::MASK_OP) >> Self::OFFSET_OP).unwrap()
    }

    pub fn set_op_code(self, op: Operation) -> Self {
        let op = (op as u16) << Self::OFFSET_OP;
        Flags(self.0 | op)
    }

    pub fn is_authoritative(&self) -> bool {
        self.0 & Self::MASK_AA > 0
    }

    pub fn set_authoritative(self) -> Self {
        Flags(self.0 | Self::MASK_AA)
    }

    pub fn is_truncated(&self) -> bool {
        self.0 & Self::MASK_TC > 0
    }

    pub fn set_truncated(self) -> Self {
        Flags(self.0 | Self::MASK_TC)
    }

    pub fn is_recursion_desired(&self) -> bool {
        self.0 & Self::MASK_RD > 0
    }

    pub fn set_recursion_desired(self) -> Self {
        Flags(self.0 | Self::MASK_RD)
    }

    pub fn is_recursion_available(&self) -> bool {
        self.0 & Self::MASK_RA > 0
    }

    pub fn set_recursion_available(self) -> Self {
        Flags(self.0 | Self::MASK_RA)
    }

    pub fn get_response_code(&self) -> ResponseCode {
        ResponseCode::try_from((self.0 & Self::MASK_RC) >> Self::OFFSET_RC).unwrap()
    }

    pub fn set_response_code(self, code: ResponseCode) -> Self {
        let code = (code as u16) << Self::OFFSET_RC;
        Flags(self.0 | code)
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

impl TryFrom<u16> for Flags {
    type Error = MtopError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        // Ensure that operation and response code are valid values but
        // otherwise use the value as is. The rest of the fields are on/off
        // bits so any combination is valid even if they don't make sense.
        let _op = Operation::try_from((value & Self::MASK_OP) >> Self::OFFSET_OP)?;
        let _rc = ResponseCode::try_from((value & Self::MASK_RC) >> Self::OFFSET_RC)?;
        Ok(Flags(value))
    }
}

impl Debug for Flags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let qr = (self.0 & Self::MASK_QR) >> Self::OFFSET_QR;
        let op = Operation::try_from((self.0 & Self::MASK_OP) >> Self::OFFSET_OP).unwrap();
        let aa = (self.0 & Self::MASK_AA) >> Self::OFFSET_AA;
        let tc = (self.0 & Self::MASK_TC) >> Self::OFFSET_TC;
        let rd = (self.0 & Self::MASK_RD) >> Self::OFFSET_RD;
        let ra = (self.0 & Self::MASK_RA) >> Self::OFFSET_RA;
        let rc = ResponseCode::try_from((self.0 & Self::MASK_RC) >> Self::OFFSET_RC).unwrap();

        write!(
            f,
            "Flags{{qr = {qr}, op = {op:?}, aa = {aa}, tc = {tc}, rd = {rd}, ra = {ra}, rc = {rc:?}}}"
        )
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u16)]
pub enum ResponseCode {
    NoError = 0,
    FormatError = 1,
    ServerFailure = 2,
    NameError = 3,
    NotImplemented = 4,
    Refused = 5,
    YxDomain = 6,
    YxRrSet = 7,
    NxRrSet = 8,
    NotAuth = 9,
    NotZone = 10,
    BadVersion = 16,
}

impl Default for ResponseCode {
    fn default() -> Self {
        Self::NoError
    }
}

impl TryFrom<u16> for ResponseCode {
    type Error = MtopError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ResponseCode::NoError),
            1 => Ok(ResponseCode::FormatError),
            2 => Ok(ResponseCode::ServerFailure),
            3 => Ok(ResponseCode::NameError),
            4 => Ok(ResponseCode::NotImplemented),
            5 => Ok(ResponseCode::Refused),
            6 => Ok(ResponseCode::YxDomain),
            7 => Ok(ResponseCode::YxRrSet),
            8 => Ok(ResponseCode::NxRrSet),
            9 => Ok(ResponseCode::NotAuth),
            10 => Ok(ResponseCode::NotZone),
            16 => Ok(ResponseCode::BadVersion),
            _ => Err(MtopError::runtime(format!(
                "invalid or unsupported response code {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u16)]
pub enum Operation {
    Query = 0,
    IQuery = 1,
    Status = 2,
    Notify = 4,
    Update = 5,
}

impl Default for Operation {
    fn default() -> Self {
        Self::Query
    }
}

impl TryFrom<u16> for Operation {
    type Error = MtopError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Operation::Query),
            1 => Ok(Operation::IQuery),
            2 => Ok(Operation::Status),
            4 => Ok(Operation::Notify),
            5 => Ok(Operation::Update),
            _ => Err(MtopError::runtime(format!(
                "invalid or unsupported operation {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Question {
    name: Name,
    qtype: RecordType,
    qclass: RecordClass,
}

impl Question {
    pub fn new(name: Name, qtype: RecordType) -> Self {
        Self {
            name,
            qtype,
            qclass: RecordClass::INET,
        }
    }

    pub fn size(&self) -> usize {
        self.name.size() + self.qtype.size() + self.qclass.size()
    }

    pub fn set_qclass(mut self, qclass: RecordClass) -> Self {
        self.qclass = qclass;
        self
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub fn qtype(&self) -> RecordType {
        self.qtype
    }

    pub fn qclass(&self) -> RecordClass {
        self.qclass
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        self.name.write_network_bytes(&mut buf)?;
        buf.write_u16::<NetworkEndian>(self.qtype.into())?;
        Ok(buf.write_u16::<NetworkEndian>(self.qclass.into())?)
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let name = Name::read_network_bytes(&mut buf)?;
        let qtype = RecordType::from(buf.read_u16::<NetworkEndian>()?);
        let qclass = RecordClass::from(buf.read_u16::<NetworkEndian>()?);
        Ok(Self { name, qtype, qclass })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Record {
    name: Name,
    rtype: RecordType,
    rclass: RecordClass,
    ttl: u32,
    rdata: RecordData,
}

impl Record {
    pub fn new(name: Name, rtype: RecordType, rclass: RecordClass, ttl: u32, rdata: RecordData) -> Self {
        Self {
            name,
            rtype,
            rclass,
            ttl,
            rdata,
        }
    }

    pub fn size(&self) -> usize {
        self.name.size()
            + self.rtype.size()
            + self.rclass.size()
            + 4 // ttl
            + 2 // rdata length
            + self.rdata.size()
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub fn rtype(&self) -> RecordType {
        self.rtype
    }

    pub fn rclass(&self) -> RecordClass {
        self.rclass
    }

    pub fn ttl(&self) -> u32 {
        self.ttl
    }

    pub fn rdata(&self) -> &RecordData {
        &self.rdata
    }

    pub fn write_network_bytes<T>(&self, mut buf: T) -> Result<(), MtopError>
    where
        T: WriteBytesExt,
    {
        // It shouldn't be possible for rdata to overflow u16 so if we do, that's a bug.
        let size = self.rdata.size();
        assert!(
            size <= usize::from(u16::MAX),
            "rdata length of {} bytes exceeds max of {} bytes",
            size,
            u16::MAX
        );

        self.name.write_network_bytes(&mut buf)?;
        buf.write_u16::<NetworkEndian>(self.rtype.into())?;
        buf.write_u16::<NetworkEndian>(self.rclass.into())?;
        buf.write_u32::<NetworkEndian>(self.ttl)?;
        buf.write_u16::<NetworkEndian>(size as u16)?;
        self.rdata.write_network_bytes(&mut buf)
    }

    pub fn read_network_bytes<T>(mut buf: T) -> Result<Self, MtopError>
    where
        T: ReadBytesExt + Seek,
    {
        let name = Name::read_network_bytes(&mut buf)?;
        let rtype = RecordType::from(buf.read_u16::<NetworkEndian>()?);
        let rclass = RecordClass::from(buf.read_u16::<NetworkEndian>()?);
        let ttl = buf.read_u32::<NetworkEndian>()?;
        let rdata_len = buf.read_u16::<NetworkEndian>()?;
        let rdata = RecordData::read_network_bytes(rtype, rdata_len, &mut buf)?;

        Ok(Self {
            name,
            rtype,
            rclass,
            ttl,
            rdata,
        })
    }
}

#[cfg(test)]
mod test {
    use super::{Flags, Header, Message, MessageId, Operation, Question, Record, ResponseCode};
    use crate::dns::core::{RecordClass, RecordType};
    use crate::dns::name::Name;
    use crate::dns::rdata::{RecordData, RecordDataA, RecordDataSRV};
    use std::io::Cursor;
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    #[rustfmt::skip]
    #[test]
    fn test_message_write_network_bytes() {
        let question = Question::new(Name::from_str("_cache._tcp.example.com.").unwrap(), RecordType::SRV);
        let answer_rdata = RecordData::SRV(RecordDataSRV::new(
            10,
            10,
            11211,
            Name::from_str("cache01.example.com.").unwrap(),
        ));
        let answer = Record::new(
            Name::from_str("_cache._tcp.example.com.").unwrap(),
            RecordType::SRV,
            RecordClass::INET,
            300,
            answer_rdata,
        );
        let extra_rdata = RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100)));
        let extra = Record::new(
            Name::from_str("cache01.example.com.").unwrap(),
            RecordType::A,
            RecordClass::INET,
            60,
            extra_rdata,
        );

        let message = Message::new(
            MessageId::from(65333), Flags::default()
                .set_response()
                .set_op_code(Operation::Query)
                .set_response_code(ResponseCode::NoError))
            .add_question(question)
            .add_answer(answer)
            .add_extra(extra);

        let mut cur = Cursor::new(Vec::new());
        message.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                // Header
                255, 53, // ID
                128, 0,  // Flags: response, query op, no error
                0, 1,    // questions
                0, 1,    // answers
                0, 0,    // authority
                0, 1,    // extra

                // Question
                6,                                // length
                95, 99, 97, 99, 104, 101,         // "_cache"
                4,                                // length
                95, 116, 99, 112,                 // "_tcp"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
                0, 33,                            // record type, SRV
                0, 1,                             // record class, INET

                // Answer
                6,                                // length
                95, 99, 97, 99, 104, 101,         // "_cache"
                4,                                // length
                95, 116, 99, 112,                 // "_tcp"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
                0, 33,                            // record type, SRV
                0, 1,                             // record class, INET
                0, 0, 1, 44,                      // TTL
                0, 27,                            // rdata size
                0, 10,                            // priority
                0, 10,                            // weight
                43, 203,                          // port
                7,                                // length
                99, 97, 99, 104, 101, 48, 49,     // "cache01"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root

                // Extra
                7,                                // length
                99, 97, 99, 104, 101, 48, 49,     // "cache01"
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
                0, 1,                             // record type, A
                0, 1,                             // record class, INET
                0, 0, 0, 60,                      // TTL
                0, 4,                             // rdata size
                127, 0, 0, 100,                   // rdata, A address
            ],
            buf,
        );
    }

    #[rustfmt::skip]
    #[test]
    fn test_message_read_network_bytes() {
        let cur = Cursor::new(vec![
            // Header
            255, 53, // ID
            128, 0,  // Flags: response, query op, no error
            0, 1,    // questions
            0, 1,    // answers
            0, 0,    // authority
            0, 1,    // extra

            // Question
            6,                                // length
            95, 99, 97, 99, 104, 101,         // "_cache"
            4,                                // length
            95, 116, 99, 112,                 // "_tcp"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 33,                            // record type, SRV
            0, 1,                             // record class, INET

            // Answer
            6,                                // length
            95, 99, 97, 99, 104, 101,         // "_cache"
            4,                                // length
            95, 116, 99, 112,                 // "_tcp"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 33,                            // record type, SRV
            0, 1,                             // record class, INET
            0, 0, 1, 44,                      // TTL
            0, 27,                            // rdata size
            0, 10,                            // priority
            0, 10,                            // weight
            43, 203,                          // port
            7,                                // length
            99, 97, 99, 104, 101, 48, 49,     // "cache01"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root

            // Extra
            7,                                // length
            99, 97, 99, 104, 101, 48, 49,     // "cache01"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 1,                             // record type, A
            0, 1,                             // record class, INET
            0, 0, 0, 60,                      // TTL
            0, 4,                             // rdata size
            127, 0, 0, 100,                   // rdata, A address
        ]);

        let message = Message::read_network_bytes(cur).unwrap();
        assert_eq!(MessageId::from(65333), message.id());
        assert_eq!(
            Flags::default()
                .set_response()
                .set_response_code(ResponseCode::NoError)
                .set_op_code(Operation::Query),
            message.flags()
        );

        let questions = message.questions();
        assert_eq!("_cache._tcp.example.com.", questions[0].name().to_string());
        assert_eq!(RecordType::SRV, questions[0].qtype());
        assert_eq!(RecordClass::INET, questions[0].qclass());

        let answers = message.answers();
        assert_eq!("_cache._tcp.example.com.", answers[0].name().to_string());
        assert_eq!(RecordType::SRV, answers[0].rtype());
        assert_eq!(RecordClass::INET, answers[0].rclass());
        assert_eq!(300, answers[0].ttl());

        if let RecordData::SRV(rd) = answers[0].rdata() {
            assert_eq!(10, rd.weight());
            assert_eq!(10, rd.priority());
            assert_eq!(11211, rd.port());
            assert_eq!("cache01.example.com.", rd.target().to_string());
        } else {
            panic!("unexpected record data type: {:?}", answers[0].rdata());
        }

        let extra = message.extra();
        assert_eq!("cache01.example.com.", extra[0].name().to_string());
        assert_eq!(RecordType::A, extra[0].rtype());
        assert_eq!(RecordClass::INET, extra[0].rclass());
        assert_eq!(60, extra[0].ttl());

        if let RecordData::A(rd) = extra[0].rdata() {
            assert_eq!(Ipv4Addr::new(127, 0, 0, 100), rd.addr());
        } else {
            panic!("unexpected record data type: {:?}", extra[0].rdata());
        }
    }

    #[rustfmt::skip]
    #[test]
    fn test_header_write_network_bytes() {
        let h = Header {
            id: MessageId::from(65333),
            flags: Flags::default().set_recursion_desired(),
            num_questions: 1,
            num_answers: 2,
            num_authority: 3,
            num_extra: 4,
        };
        let mut cur = Cursor::new(Vec::new());
        h.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                255, 53, // ID
                1, 0,    // Flags, recursion desired
                0, 1,    // questions
                0, 2,    // answers
                0, 3,    // authority
                0, 4,    // extra
            ],
            buf,
        )
    }

    #[rustfmt::skip]
    #[test]
    fn test_header_read_network_bytes() {
        let cur = Cursor::new(vec![
            255, 53, // ID
            1, 0,    // Flags, recursion desired
            0, 1,    // questions
            0, 2,    // answers,
            0, 3,    // authority
            0, 4,    // extra
        ]);

        let h = Header::read_network_bytes(cur).unwrap();
        assert_eq!(MessageId::from(65333), h.id);
        assert_eq!(Flags::default().set_recursion_desired(), h.flags);
        assert_eq!(1, h.num_questions);
        assert_eq!(2, h.num_answers);
        assert_eq!(3, h.num_authority);
        assert_eq!(4, h.num_extra);
    }

    #[test]
    fn test_flags() {
        let f = Flags::default().set_query();
        assert!(f.is_query());

        let f = Flags::default().set_response();
        assert!(f.is_response());

        let f = Flags::default().set_op_code(Operation::Notify);
        assert_eq!(Operation::Notify, f.get_op_code());

        let f = Flags::default().set_authoritative();
        assert!(f.is_authoritative());

        let f = Flags::default().set_truncated();
        assert!(f.is_truncated());

        let f = Flags::default().set_recursion_desired();
        assert!(f.is_recursion_desired());

        let f = Flags::default().set_recursion_available();
        assert!(f.is_recursion_available());

        let f = Flags::default().set_response_code(ResponseCode::ServerFailure);
        assert_eq!(ResponseCode::ServerFailure, f.get_response_code());

        let f = Flags::default()
            .set_query()
            .set_recursion_desired()
            .set_op_code(Operation::Query);
        assert!(f.is_query());
        assert!(f.is_recursion_desired());
        assert_eq!(Operation::Query, f.get_op_code());
    }

    #[rustfmt::skip]
    #[test]
    fn test_question_write_network_bytes() {
        let q = Question::new(Name::from_str("example.com.").unwrap(), RecordType::AAAA);
        let size = q.size();
        let mut cur = Cursor::new(Vec::new());
        q.write_network_bytes(&mut cur).unwrap();
        let buf = cur.into_inner();

        assert_eq!(
            vec![
                7,                                // length
                101, 120, 97, 109, 112, 108, 101, // "example"
                3,                                // length
                99, 111, 109,                     // "com"
                0,                                // root
                0, 28,                            // AAAA record
                0, 1,                             // INET class
            ],
            buf,
        );
        assert_eq!(size, buf.len());
    }

    #[rustfmt::skip]
    #[test]
    fn test_question_read_network_bytes() {
        let cur = Cursor::new(vec![
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 28,                            // AAAA record
            0, 1,                             // INET class
        ]);

        let size = cur.get_ref().len();
        let q = Question::read_network_bytes(cur).unwrap();
        assert_eq!("example.com.", q.name().to_string());
        assert_eq!(RecordType::AAAA, q.qtype());
        assert_eq!(RecordClass::INET, q.qclass());
        assert_eq!(size, q.size());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_write_network_bytes() {
        let rr = Record::new(
            Name::from_str("www.example.com.").unwrap(),
            RecordType::A,
            RecordClass::INET,
            300,
            RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100))),
        );
        let size = rr.size();
        let mut cur = Cursor::new(Vec::new());
        rr.write_network_bytes(&mut cur).unwrap();
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
                0, 1,                             // record type, A
                0, 1,                             // record class, INET
                0, 0, 1, 44,                      // TTL
                0, 4,                             // rdata size
                127, 0, 0, 100,                   // rdata, A address
            ],
            buf,
        );
        assert_eq!(size, buf.len());
    }

    #[rustfmt::skip]
    #[test]
    fn test_record_read_network_bytes() {
        let cur = Cursor::new(vec![
            3,                                // length
            119, 119, 119,                    // "www"
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 1,                             // record type, A
            0, 1,                             // record class, INET
            0, 0, 1, 44,                      // TTL
            0, 4,                             // rdata size
            127, 0, 0, 100,                   // rdata, A address
        ]);

        let size = cur.get_ref().len();
        let rr = Record::read_network_bytes(cur).unwrap();
        assert_eq!("www.example.com.", rr.name().to_string());
        assert_eq!(RecordType::A, rr.rtype());
        assert_eq!(RecordClass::INET, rr.rclass());
        assert_eq!(300, rr.ttl());
        if let RecordData::A(rd) = rr.rdata() {
            assert_eq!(Ipv4Addr::new(127, 0, 0, 100), rd.addr());
        } else {
            panic!("unexpected rdata type: {:?}", rr.rdata());
        }
        assert_eq!(size, rr.size());
    }
}
