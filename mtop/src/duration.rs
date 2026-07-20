use mtop_client::MtopError;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct DurationString {
    parts: Vec<(u64, Unit)>,
    total: Duration,
}

impl DurationString {
    pub fn must(s: &str) -> Self {
        s.parse().unwrap()
    }

    pub fn as_duration(&self) -> Duration {
        self.total
    }
}

impl FromStr for DurationString {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let chunks: Vec<&str> = s
            .as_bytes()
            .chunk_by(|a, b| a.is_ascii_digit() == b.is_ascii_digit())
            .flat_map(|v| str::from_utf8(v))
            .collect();

        if !chunks.len().is_multiple_of(2) {
            return Err(MtopError::configuration(format!(
                "all values must have a corresponding unit. got '{}'",
                s
            )));
        }

        let mut parts = Vec::with_capacity(chunks.len() / 2);
        let mut total = Duration::ZERO;
        let mut expect_val = true;
        let mut last_val = 0;

        for chunk in chunks {
            if expect_val {
                last_val = chunk
                    .parse()
                    .map_err(|e| MtopError::configuration_cause(format!("cannot parse {} from {}", chunk, s), e))?;
                expect_val = false;
            } else {
                let unit: Unit = chunk.parse()?;
                let duration = unit.as_duration(last_val)?;

                total = total.checked_add(duration).ok_or_else(|| {
                    MtopError::configuration(format!("{}{} in {} overflows max duration", last_val, unit, s))
                })?;

                parts.push((last_val, unit));
                expect_val = true;
            }
        }

        Ok(Self { parts, total })
    }
}

impl fmt::Display for DurationString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (val, unit) in &self.parts {
            val.fmt(f)?;
            unit.fmt(f)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Unit {
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Unit {
    const MAX_HOURS: u64 = u64::MAX / 3600;
    const MAX_MINUTES: u64 = u64::MAX / 60;

    fn as_duration(&self, val: u64) -> Result<Duration, MtopError> {
        match self {
            Unit::Hour => {
                if val >= Self::MAX_HOURS {
                    Err(MtopError::configuration(format!("overflowing value {}{}", val, self)))
                } else {
                    Ok(Duration::from_hours(val))
                }
            }
            Unit::Minute => {
                if val >= Self::MAX_MINUTES {
                    Err(MtopError::configuration(format!("overflowing value {}{}", val, self)))
                } else {
                    Ok(Duration::from_mins(val))
                }
            }
            Unit::Second => Ok(Duration::from_secs(val)),
            Unit::Millisecond => Ok(Duration::from_millis(val)),
            Unit::Microsecond => Ok(Duration::from_micros(val)),
            Unit::Nanosecond => Ok(Duration::from_nanos(val)),
        }
    }
}

impl FromStr for Unit {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "h" => Ok(Unit::Hour),
            "m" => Ok(Unit::Minute),
            "s" => Ok(Unit::Second),
            "ms" => Ok(Unit::Millisecond),
            "us" => Ok(Unit::Microsecond),
            "ns" => Ok(Unit::Nanosecond),
            _ => Err(MtopError::configuration(format!("invalid unit {}", s))),
        }
    }
}

impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unit::Hour => "h".fmt(f),
            Unit::Minute => "m".fmt(f),
            Unit::Second => "s".fmt(f),
            Unit::Millisecond => "ms".fmt(f),
            Unit::Microsecond => "us".fmt(f),
            Unit::Nanosecond => "ns".fmt(f),
        }
    }
}

#[cfg(test)]
mod test {
    use super::DurationString;
    use mtop_client::MtopError;
    use std::time::Duration;

    fn parse_to_duration(s: &str) -> Result<Duration, MtopError> {
        let cfg = s.parse::<DurationString>()?;
        Ok(cfg.as_duration())
    }

    #[test]
    fn test_try_from_string_single_values() {
        assert_eq!(Duration::from_nanos(5_000), parse_to_duration("5000ns").unwrap());
        assert_eq!(Duration::from_micros(100), parse_to_duration("100us").unwrap());
        assert_eq!(Duration::from_millis(200), parse_to_duration("200ms").unwrap());
        assert_eq!(Duration::from_secs(90), parse_to_duration("90s").unwrap());
        assert_eq!(Duration::from_mins(1), parse_to_duration("1m").unwrap());
        assert_eq!(Duration::from_hours(1), parse_to_duration("1h").unwrap());
    }

    #[test]
    fn test_try_from_string_multiple_values() {
        assert_eq!(Duration::from_nanos(1_005_000), parse_to_duration("1ms5000ns").unwrap());
        assert_eq!(Duration::from_micros(1_200), parse_to_duration("1ms200us").unwrap());
        assert_eq!(Duration::from_millis(1500), parse_to_duration("1s500ms").unwrap());
        assert_eq!(Duration::from_secs(90), parse_to_duration("1m30s").unwrap());
        assert_eq!(Duration::from_mins(70), parse_to_duration("1h10m").unwrap());
    }

    #[test]
    fn test_try_from_string_invalid_values() {
        assert!(parse_to_duration("-12").is_err());
        assert!(parse_to_duration("-3h").is_err());
        assert!(parse_to_duration("asdf").is_err());
        assert!(parse_to_duration("23").is_err());
        assert!(parse_to_duration("1h7").is_err());
        assert!(parse_to_duration("1y").is_err());
        assert!(parse_to_duration("4d").is_err());
        assert!(parse_to_duration("1🫠").is_err());
        assert!(parse_to_duration("9999999999999999999h").is_err());
    }
}
