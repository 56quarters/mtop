use crate::core::MtopError;

/// Convert the ASCII byte of a hex digit to the actual digit.
fn hex_to_digit(hex: u8) -> Option<u8> {
    match hex {
        b'0'..=b'9' => Some(hex - b'0'),
        b'A'..=b'F' => Some(hex - b'A' + 10),
        b'a'..=b'f' => Some(hex - b'a' + 10),
        _ => None,
    }
}

/// Covert the lower four bits of a byte to an ASCII byte of a hex digit
/// representing the number.
fn digit_to_hex(digit: u8) -> u8 {
    match digit {
        0..=9 => digit + b'0',
        10..=15 => digit + b'A' - 10,
        _ => panic!("unexpected digit to convert to hex '{}'", digit),
    }
}

/// Decode a percent-encoded (aka URL-encoded) string.
///
/// Incomplete or invalid encodings are included in the decoded string verbatim.
///
/// # Example
/// ```
/// # use mtop_client::url_decode;
/// assert_eq!("no-encoding", url_decode("no-encoding").unwrap());
/// assert_eq!("either/or", url_decode("either%2For").unwrap());
/// assert_eq!("have not", url_decode("have%20not").unwrap());
/// assert_eq!("up by 10%", url_decode("up%20by%2010%25").unwrap());
/// assert_eq!("invalid%2%encoding", url_decode("invalid%2%encoding").unwrap());
/// ```
pub fn url_decode(s: &str) -> Result<String, MtopError> {
    // Short-circuit for a fairly common case, this string isn't actually URL encoded.
    if !s.contains('%') {
        return Ok(s.to_owned());
    }

    let mut out = Vec::with_capacity(s.len());
    let mut data = s.as_bytes();
    loop {
        let mut parts = data.splitn(2, |&b| b == b'%');

        // Append anything preceding the '%' with a single call to the output slice instead
        // of looping over the entire input one byte at a time.
        if let Some(plain) = parts.next() {
            out.extend_from_slice(plain);
        }

        // A 'None' remainder means there was no '%' and we've finished parsing the input.
        let remainder = parts.next();
        if remainder.is_none() {
            break;
        }

        data = remainder.unwrap();
        // An empty slice remainder means there was a trailing '%' on the input so we need
        // to add it to the output before returning.
        if data.is_empty() {
            out.push(b'%');
            break;
        }

        let char1 = data[0];
        let Some(digit1) = hex_to_digit(char1) else {
            // Not a valid escape sequence. Add anything we've already consumed to
            // the output before moving on to the next iteration of the parsing loop.
            out.push(b'%');
            continue;
        };

        data = &data[1..];
        if data.is_empty() {
            // If we're aborting early, add anything we've already consumed to the output.
            out.push(b'%');
            out.push(char1);
            break;
        }

        let char2 = data[0];
        let Some(digit2) = hex_to_digit(char2) else {
            // Not a valid escape sequence. Add anything we've already consumed to
            // the output before moving on to the next iteration of the parsing loop.
            out.push(b'%');
            out.push(char1);
            continue;
        };

        let c = digit1 << 4 | digit2;
        out.push(c);

        data = &data[1..];
    }

    String::from_utf8(out).map_err(|e| MtopError::runtime_cause(format!("invalid url encoding in '{}'", s), e))
}

/// Encode any characters that aren't permitted within a URL using percent-encoding.
///
/// Any characters besides 0-9, A-Z, a-z, `-`, `_`, `.`, `~` are encoded.
///
/// # Example
/// ```
/// # use mtop_client::url_encode;
/// assert_eq!("no-encoding", url_encode("no-encoding"));
/// assert_eq!("either%2For", url_encode("either/or"));
/// assert_eq!("some%20code", url_encode("some code"));
/// assert_eq!("sugar%26spice", url_encode("sugar&spice"));
/// assert_eq!("x%21%3Dy", url_encode("x!=y"));
/// assert_eq!("09az%2009AZ", url_encode("09az 09AZ"));
///```
pub fn url_encode(s: &str) -> String {
    let mut data = s.as_bytes();
    // If we have to encode any characters, the output will take more space
    // than the input. Round up to the next power of two to try to avoid having
    // to allocate for most inputs. There's not much science behind this number.
    let mut out = Vec::with_capacity(data.len().next_power_of_two());

    loop {
        if data.is_empty() {
            break;
        }

        match data[0] {
            byte @ (b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'-' | b'_' | b'.' | b'~') => out.push(byte),
            byte => {
                let hex1 = digit_to_hex(byte >> 4);
                let hex2 = digit_to_hex(0x0F & byte);
                out.extend_from_slice(&[b'%', hex1, hex2]);
            }
        }

        data = &data[1..];
    }

    // SAFETY: Output is guaranteed to only consist of a limited set of ASCII bytes and
    // percent and hex digit escape sequences.
    unsafe { String::from_utf8_unchecked(out) }
}

#[cfg(test)]
mod test {
    use super::{url_decode, url_encode};

    #[test]
    fn test_url_decode_simple() {
        assert_eq!("no-encoding", url_decode("no-encoding").unwrap());
        assert_eq!("either/or", url_decode("either%2for").unwrap());
        assert_eq!("some code", url_decode("some%20code").unwrap());
        assert_eq!("sugar&spice", url_decode("sugar%26spice").unwrap());
        assert_eq!("x!=y", url_decode("x%21%3Dy").unwrap());
        assert_eq!("09az 09AZ", url_decode("09az%2009AZ").unwrap());
    }

    #[test]
    fn test_url_decode_emoji() {
        assert_eq!(
            "🫠 this is fine",
            url_decode("%F0%9F%AB%A0%20this%20is%20fine").unwrap()
        );
    }

    #[test]
    fn test_url_decode_unencoded_emoji_skipped() {
        assert_eq!("🫠 this is fine", url_decode("🫠 this is fine").unwrap());
    }

    #[test]
    fn test_url_decode_broken_encoding() {
        assert_eq!("some code", url_decode("some%20code").unwrap());
        assert_eq!("some code%", url_decode("some%20code%").unwrap());
        assert_eq!("some code%2", url_decode("some%20code%2").unwrap());
        assert_eq!("some code%%", url_decode("some%20code%%").unwrap());
        assert_eq!("some code ", url_decode("some%20code%20").unwrap());
        assert_eq!("some code%2G", url_decode("some%20code%2G").unwrap());
    }

    #[test]
    fn test_url_decode_invalid_utf8() {
        assert!(
            url_decode("%FF").is_err(),
            "expected invalid utf-8 byte 0xFF to be an error"
        );
    }

    #[test]
    fn test_url_encode_simple() {
        assert_eq!("no-encoding", url_encode("no-encoding"));
        assert_eq!("either%2For", url_encode("either/or"));
        assert_eq!("some%20code", url_encode("some code"));
        assert_eq!("sugar%26spice", url_encode("sugar&spice"));
        assert_eq!("x%21%3Dy", url_encode("x!=y"));
        assert_eq!("09az%2009AZ", url_encode("09az 09AZ"));
    }

    #[test]
    fn test_url_encode_emoji() {
        assert_eq!("%F0%9F%AB%A0%20this%20is%20fine", url_encode("🫠 this is fine"));
    }
}
