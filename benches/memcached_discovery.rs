use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use mtop_client::dns::{
    DnsClient, Flags, Message, MessageId, Name, Question, Record, RecordClass, RecordData, RecordDataA, RecordDataSRV,
    RecordType,
};
use mtop_client::{Discovery, MtopError};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

static RT: OnceLock<Runtime> = OnceLock::new();

#[derive(Debug, Default)]
struct MockDnsClient {
    responses: HashMap<(Name, RecordType, RecordClass), Message>,
}

#[async_trait]
impl DnsClient for MockDnsClient {
    async fn resolve(
        &self,
        id: MessageId,
        name: Name,
        rtype: RecordType,
        rclass: RecordClass,
    ) -> Result<Message, MtopError> {
        match self.responses.get(&(name, rtype, rclass)) {
            Some(v) => Ok(v.clone().set_id(id)),
            None => Err(MtopError::runtime("no message available")),
        }
    }
}

fn new_response_srv() -> Message {
    let flags = Flags::default()
        .set_response()
        .set_recursion_desired()
        .set_recursion_available();

    Message::new(MessageId::random(), flags)
        .add_question(Question::new(
            Name::from_str("_memcached._tcp.example.com").unwrap(),
            RecordType::SRV,
        ))
        .add_answer(Record::new(
            Name::from_str("cache01.example.com").unwrap(),
            RecordType::SRV,
            RecordClass::INET,
            300,
            RecordData::SRV(RecordDataSRV::new(
                10,
                10,
                11211,
                Name::from_str("cache01.example.com").unwrap(),
            )),
        ))
        .add_answer(Record::new(
            Name::from_str("cache02.example.com").unwrap(),
            RecordType::SRV,
            RecordClass::INET,
            300,
            RecordData::SRV(RecordDataSRV::new(
                10,
                10,
                11211,
                Name::from_str("cache02.example.com").unwrap(),
            )),
        ))
}

fn new_response_a() -> Message {
    let flags = Flags::default()
        .set_response()
        .set_recursion_desired()
        .set_recursion_available();

    Message::new(MessageId::random(), flags)
        .add_question(Question::new(
            Name::from_str("cache.example.com").unwrap(),
            RecordType::A,
        ))
        .add_answer(Record::new(
            Name::from_str("cache01.example.com").unwrap(),
            RecordType::A,
            RecordClass::INET,
            300,
            RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 1))),
        ))
        .add_answer(Record::new(
            Name::from_str("cache02.example.com").unwrap(),
            RecordType::A,
            RecordClass::INET,
            300,
            RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 2))),
        ))
}

fn new_response_aaaa() -> Message {
    let flags = Flags::default()
        .set_response()
        .set_recursion_desired()
        .set_recursion_available();

    // Stop trying to make ~fetch~ IPv6 happen.
    Message::new(MessageId::random(), flags).add_question(Question::new(
        Name::from_str("cache.example.com").unwrap(),
        RecordType::AAAA,
    ))
}

fn memcached_discovery_resolve_by_proto(c: &mut Criterion) {
    let runtime = RT.get_or_init(|| Runtime::new().unwrap());

    c.bench_function("Discovery::resolve_by_proto:srv", |b| {
        b.iter_batched(
            || {
                let mut client = MockDnsClient::default();
                client.responses.insert(
                    (
                        Name::from_str("_memcached._tcp.example.com").unwrap(),
                        RecordType::SRV,
                        RecordClass::INET,
                    ),
                    new_response_srv(),
                );
                Discovery::new(client)
            },
            |discovery| {
                runtime.block_on(async {
                    let _ = discovery
                        .resolve_by_proto("dnssrv+_memcached._tcp.example.com:11211")
                        .await
                        .unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("Discovery::resolve_by_proto:a_aaaa", |b| {
        b.iter_batched(
            || {
                let mut client = MockDnsClient::default();
                client.responses.insert(
                    (
                        Name::from_str("cache.example.com").unwrap(),
                        RecordType::A,
                        RecordClass::INET,
                    ),
                    new_response_a(),
                );
                client.responses.insert(
                    (
                        Name::from_str("cache.example.com").unwrap(),
                        RecordType::AAAA,
                        RecordClass::INET,
                    ),
                    new_response_aaaa(),
                );

                Discovery::new(client)
            },
            |discovery| {
                runtime.block_on(async {
                    let _ = discovery.resolve_by_proto("dns+cache.example.com:11211").await.unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("Discovery::resolve_by_proto:socket_addr", |b| {
        b.iter_batched(
            || Discovery::new(MockDnsClient::default()),
            |discovery| {
                runtime.block_on(async {
                    let _ = discovery.resolve_by_proto("127.0.0.1:11211").await.unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(memcached_discovery, memcached_discovery_resolve_by_proto);
criterion_main!(memcached_discovery);
