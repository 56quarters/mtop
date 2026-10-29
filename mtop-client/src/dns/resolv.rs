use crate::core::MtopError;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::range::RangeInclusive;
use std::str::FromStr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

const DEFAULT_PORT: u16 = 53;
const MAX_NAMESERVERS: usize = 3;

/// Configuration for a DNS client based on a parsed resolv.conf file.
///
/// Note that only the `nameserver` setting and a few `option`s are supported.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ResolvConf {
    pub nameservers: Vec<SocketAddr>,
    pub options: ResolvConfOptions,
}

/// Options to change the behavior of a DNS client based on a resolv.conf file.
///
/// Note that only a subset of options are supported.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ResolvConfOptions {
    pub timeout: Option<Duration>,
    pub attempts: Option<u64>,
    pub rotate: Option<bool>,
}

/// Read settings for a DNS client from a resolv.conf configuration file.
pub async fn config<R>(read: R) -> Result<ResolvConf, MtopError>
where
    R: AsyncRead + Send + Sync + Unpin + 'static,
{
    let mut reader = BufReader::new(read);
    let mut conf = ResolvConf::default();
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }

        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            tracing::debug!(message = "skipping malformed resolv.conf line", line = line);
            continue;
        };

        match Token::get(key) {
            Some(Token::NameServer) => {
                if conf.nameservers.len() < MAX_NAMESERVERS {
                    conf.nameservers.push(parse_nameserver(line, parts)?);
                }
            }
            Some(Token::Options) => {
                for opt in parse_options(parts)? {
                    match opt {
                        OptionsToken::Timeout(t) => {
                            conf.options.timeout = Some(Duration::from_secs(t));
                        }
                        OptionsToken::Attempts(n) => {
                            conf.options.attempts = Some(n);
                        }
                        OptionsToken::Rotate => {
                            conf.options.rotate = Some(true);
                        }
                        OptionsToken::Unknown(name) => {
                            tracing::debug!(
                                message = "skipping unknown resolv.conf option",
                                option = name,
                                line = line
                            );
                        }
                    }
                }
            }
            None => {
                tracing::debug!(
                    message = "skipping unknown resolv.conf setting",
                    setting = key,
                    line = line
                );
            }
        }
    }

    Ok(conf)
}

/// Parse a single nameserver IP address, adding a default port of 53, from a `nameserver`
/// line in a resolv.conf file, returning an error if the address is malformed.
fn parse_nameserver<'a>(line: &str, mut parts: impl Iterator<Item = &'a str>) -> Result<SocketAddr, MtopError> {
    if let Some(part) = parts.next() {
        part.parse::<IpAddr>()
            .map(|ip| (ip, DEFAULT_PORT).into())
            .map_err(|e| MtopError::configuration_cause(format!("malformed nameserver address '{}'", part), e))
    } else {
        Err(MtopError::configuration(format!(
            "malformed nameserver configuration '{}'",
            line
        )))
    }
}

/// Parse one or more options from an `option` line in a resolv.conf file, ignoring
/// unsupported options but aborting on supported but malformed options.
fn parse_options<'a>(parts: impl Iterator<Item = &'a str>) -> Result<Vec<OptionsToken>, MtopError> {
    let mut out = Vec::new();

    for part in parts {
        out.push(part.parse()?);
    }

    Ok(out)
}

/// Top-level configuration setting in a resolv.conf file.
///
/// Note that only a subset of all possible settings are supported.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum Token {
    NameServer,
    Options,
}

impl Token {
    fn get(s: &str) -> Option<Self> {
        match s {
            "nameserver" => Some(Self::NameServer),
            "options" => Some(Self::Options),
            _ => None,
        }
    }
}

/// Keyword or key-value pair associated with an option token.
///
/// Note that only a subset of all possible options are supported.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum OptionsToken {
    Timeout(u64),
    Attempts(u64),
    Rotate,
    Unknown(String),
}

impl OptionsToken {
    const TIMEOUT_RANGE: RangeInclusive<u64> = RangeInclusive { start: 1, last: 30 };
    const ATTEMPTS_RANGE: RangeInclusive<u64> = RangeInclusive { start: 1, last: 5 };

    fn parse_range(key: &str, val: &str, rng: RangeInclusive<u64>) -> Result<u64, MtopError> {
        let n: u64 = val
            .parse()
            .map_err(|e| MtopError::configuration_cause(format!("unable to parse {} value '{}'", key, val), e))?;

        if n > rng.last {
            Ok(rng.last)
        } else if n < rng.start {
            Err(MtopError::configuration(format!(
                "out of bounds {} value '{}': minimum is {}",
                key, val, rng.start
            )))
        } else {
            Ok(n)
        }
    }
}

impl FromStr for OptionsToken {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "rotate" {
            Ok(Self::Rotate)
        } else {
            match s.split_once(':') {
                Some((k @ "timeout", v)) => Ok(Self::Timeout(Self::parse_range(k, v, Self::TIMEOUT_RANGE)?)),
                Some((k @ "attempts", v)) => Ok(Self::Attempts(Self::parse_range(k, v, Self::ATTEMPTS_RANGE)?)),
                _ => Ok(Self::Unknown(s.to_owned())),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{OptionsToken, Token, config};
    use crate::core::ErrorKind;
    use crate::dns::{ResolvConf, ResolvConfOptions};
    use std::io::{Cursor, Error as IOError, ErrorKind as IOErrorKind};
    use std::pin::Pin;
    use std::str::FromStr;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, ReadBuf};

    #[test]
    fn test_configuration() {
        assert_eq!(Some(Token::NameServer), Token::get("nameserver"));
        assert_eq!(Some(Token::Options), Token::get("options"));
        assert_eq!(None, Token::get("invalid"));
    }

    #[test]
    fn test_configuration_option_success() {
        assert_eq!(OptionsToken::Rotate, OptionsToken::from_str("rotate").unwrap());
        assert_eq!(OptionsToken::Timeout(3), OptionsToken::from_str("timeout:3").unwrap());
        assert_eq!(OptionsToken::Attempts(4), OptionsToken::from_str("attempts:4").unwrap());
    }

    #[test]
    fn test_configuration_option_limits() {
        assert_eq!(OptionsToken::Timeout(30), OptionsToken::from_str("timeout:35").unwrap());
        assert_eq!(
            OptionsToken::Attempts(5),
            OptionsToken::from_str("attempts:10").unwrap()
        );
    }

    #[test]
    fn test_configuration_option_error() {
        assert!(OptionsToken::from_str("timeout:bad").is_err());
        assert!(OptionsToken::from_str("attempts:-5").is_err());
    }

    #[test]
    fn test_configuration_option_unknown() {
        assert_eq!(
            OptionsToken::Unknown("ndots:3".to_owned()),
            OptionsToken::from_str("ndots:3").unwrap()
        );
    }

    #[tokio::test]
    async fn test_config_read_error() {
        struct ErrAsyncRead;
        impl AsyncRead for ErrAsyncRead {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Err(IOError::new(IOErrorKind::UnexpectedEof, "test error")))
            }
        }

        let reader = ErrAsyncRead;
        let res = config(reader).await.unwrap_err();
        assert_eq!(ErrorKind::IO, res.kind());
    }

    #[tokio::test]
    async fn test_config_no_content() {
        let reader = Cursor::new(Vec::new());
        let res = config(reader).await.unwrap();
        assert_eq!(ResolvConf::default(), res);
    }

    #[tokio::test]
    async fn test_config_all_comments() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "# this is a comment\n",
            "# another comment\n",
        ));
        let res = config(reader).await.unwrap();
        assert_eq!(ResolvConf::default(), res);
    }

    #[tokio::test]
    async fn test_config_all_unsupported() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "scrambler 127.0.0.5\n",
            "invalid directive\n",
        ));
        let res = config(reader).await.unwrap();
        assert_eq!(ResolvConf::default(), res);
    }

    #[tokio::test]
    async fn test_config_nameservers_search_invalid_options() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "# this is a comment\n",
            "nameserver 127.0.0.53\n",
            "options casual-fridays:true\n",
        ));

        let expected = ResolvConf {
            nameservers: vec!["127.0.0.53:53".parse().unwrap()],
            options: ResolvConfOptions::default(),
        };

        let res = config(reader).await.unwrap();
        assert_eq!(expected, res);
    }

    #[tokio::test]
    async fn test_config_nameservers_search_no_options() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "# this is a comment\n",
            "nameserver 127.0.0.53\n",
        ));

        let expected = ResolvConf {
            nameservers: vec!["127.0.0.53:53".parse().unwrap()],
            options: ResolvConfOptions::default(),
        };

        let res = config(reader).await.unwrap();
        assert_eq!(expected, res);
    }

    #[tokio::test]
    async fn test_config_nameservers_out_of_range_low() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "# this is a comment\n",
            "nameserver 127.0.0.53\n",
            "options ndots:3 attempts:0 edns0\n",
        ));

        let res = config(reader).await;
        assert!(res.is_err(), "expected error, got {:?}", res);
    }

    #[tokio::test]
    async fn test_config_nameservers_out_of_range_high() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "# this is a comment\n",
            "nameserver 127.0.0.53\n",
            "options ndots:3 attempts:1000 timeout:60 edns0\n",
        ));

        let expected = ResolvConf {
            nameservers: vec!["127.0.0.53:53".parse().unwrap()],
            options: ResolvConfOptions {
                timeout: Some(Duration::from_secs(30)),
                attempts: Some(5),
                rotate: None,
            },
        };

        let res = config(reader).await.unwrap();
        assert_eq!(expected, res);
    }

    #[tokio::test]
    async fn test_config_nameservers_search_options() {
        #[rustfmt::skip]
        let reader = Cursor::new(concat!(
            "# this is a comment\n",
            "nameserver 127.0.0.53\n",
            "nameserver 127.0.0.54\n",
            "nameserver 127.0.0.55\n",
            "options ndots:3 attempts:5 timeout:10 rotate use-vc edns0\n",
        ));

        let expected = ResolvConf {
            nameservers: vec![
                "127.0.0.53:53".parse().unwrap(),
                "127.0.0.54:53".parse().unwrap(),
                "127.0.0.55:53".parse().unwrap(),
            ],
            options: ResolvConfOptions {
                timeout: Some(Duration::from_secs(10)),
                attempts: Some(5),
                rotate: Some(true),
            },
        };

        let res = config(reader).await.unwrap();
        assert_eq!(expected, res);
    }
}
