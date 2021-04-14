use bytes::BufMut;
use bytes::Bytes;
use memchr::memchr;
use thiserror::Error;

use std::{
    cmp::Ordering,
    convert::{TryFrom, TryInto},
    vec,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Counter,
    Timer,
    Gauge,
    DirectGauge,
    Set,
}

impl TryFrom<&[u8]> for Type {
    type Error = ParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 1 && value.len() > 2 {
            return Err(ParseError::InvalidType);
        }
        match value {
            b"c" => Ok(Type::Counter),
            b"ms" => Ok(Type::Timer),
            b"g" => Ok(Type::Gauge),
            b"G" => Ok(Type::DirectGauge),
            b"s" => Ok(Type::Set),
            _ => Err(ParseError::InvalidType),
        }
    }
}

impl From<Type> for &[u8] {
    fn from(input: Type) -> Self {
        match input {
            Type::Counter => b"c",
            Type::DirectGauge => b"G",
            Type::Gauge => b"g",
            Type::Timer => b"ms",
            Type::Set => b"s",
        }
    }
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("invalid parsed value")]
    InvalidValue,
    #[error("invalid sample rate")]
    InvalidSampleRate,
    #[error("invalid type")]
    InvalidType,
    #[error("invalid tag")]
    InvalidTag,
    #[error("overall invalid line - no structural elements found in parsing")]
    InvalidLine,
    #[error("more than one sample rate field found")]
    RepeatedSampleRate,
    #[error("more than one set of tags found")]
    RepeatedTags,
    #[error("unsupported extension field")]
    UnsupportedExtensionField,
}

/// Set of key/value fields for a tag.
#[derive(Debug, Clone, Eq)]
pub struct Tag {
    name: Vec<u8>,
    value: Vec<u8>,
}

impl Ord for Tag {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for Tag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        return self.name == other.name;
    }
}

pub trait Parsed {
    fn name(&self) -> &[u8];
    fn metric_type(&self) -> Type;
    fn value(&self) -> f64;
    fn sample_rate(&self) -> Option<f64>;
    fn tags(&self) -> &[Tag];
}

/// A structured and owned version of [`PDU`](PDU)
///
/// Gives an owned structure which represents a parsed statsd protocol unit
/// which owns all of its fields. When parsing, no canonicalization is performed
/// by default.
#[derive(Debug, Clone, PartialEq)]
pub struct Owned {
    name: Vec<u8>,
    mtype: Type,
    value: f64,
    sample_rate: Option<f64>,
    tags: Vec<Tag>,
}

impl Parsed for Owned {
    fn name(&self) -> &[u8] {
        self.name.as_ref()
    }
    fn metric_type(&self) -> Type {
        self.mtype
    }
    fn value(&self) -> f64 {
        self.value
    }
    fn sample_rate(&self) -> Option<f64> {
        self.sample_rate
    }
    fn tags(&self) -> &[Tag] {
        self.tags.as_slice()
    }
}

impl TryFrom<PDU> for Owned {
    type Error = ParseError;

    fn try_from(value: PDU) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&PDU> for Owned {
    type Error = ParseError;

    fn try_from(pdu: &PDU) -> Result<Self, Self::Error> {
        let value = match lexical::parse::<f64, _>(pdu.value()) {
            Ok(v) => v,
            Err(_) => return Err(ParseError::InvalidValue),
        };
        let sample_rate = pdu
            .sample_rate()
            .map(|sr| match lexical::parse::<f64, _>(sr) {
                Ok(v) if (v > 0.0 && v <= 1.0) => Ok(v),
                _ => Err(ParseError::InvalidSampleRate),
            })
            .transpose()?;
        let mtype: Type = pdu.pdu_type().try_into()?;
        let tags = pdu.tags().map(|v| parse_tags(v)).transpose()?;
        Ok(Owned {
            name: pdu.name().to_vec(),
            mtype: mtype,
            value: value,
            sample_rate: sample_rate,
            tags: tags.unwrap_or_default(),
        })
    }
}

impl From<Owned> for PDU {
    fn from(input: Owned) -> Self {
        (&input).into()
    }
}

impl From<&Owned> for &PDU {
    fn from(input: &Owned) -> Self {
        input.into()
    }
}

impl From<&Owned> for PDU {
    fn from(input: &Owned) -> Self {
        let mut bytes = Vec::with_capacity(input.name.len() + (input.tags.len() * 64) + 64);

        bytes.extend(&input.name);
        bytes.push(b':');
        let value_index = bytes.len();
        bytes.extend(lexical::to_string(input.value).as_bytes());
        bytes.push(b'|');
        let type_index = bytes.len();
        bytes.extend_from_slice(input.mtype.into());
        let type_index_end = bytes.len();
        let sample_rate_index = if let Some(sr) = input.sample_rate {
            bytes.extend_from_slice(b"|@");
            let start = bytes.len();
            bytes.extend(lexical::to_string(sr).as_bytes());
            let end = bytes.len();
            Some((start, end))
        } else {
            None
        };

        let tags_index = if input.tags.len() > 0 {
            bytes.extend_from_slice(b"|#");
            let start = bytes.len();
            let mut peek = input.tags.iter().peekable();
            while let Some(tag) = peek.next() {
                bytes.extend(&tag.name);
                bytes.push(b':');
                bytes.extend(&tag.value);
                if let Some(_) = peek.peek() {
                    bytes.push(b',');
                }
            }
            Some((start, bytes.len()))
        } else {
            None
        };
        PDU {
            underlying: Bytes::from(bytes),
            value_index: value_index,
            type_index: type_index,
            type_index_end: type_index_end,
            sample_rate_index: sample_rate_index,
            tags_index: tags_index,
        }
    }
}

pub mod convert {
    use super::*;
    /// Convert from external tags to internal tags. Does not check for
    /// collisions of existing inline tags with the newly generated inline tags.
    pub fn to_inline_tags(mut input: Owned) -> Owned {
        if input.tags.len() == 0 {
            return input;
        }
        input.tags.sort();
        let mut name = input.name;
        // Estimate on tag size without iterating through all actual tags
        name.reserve(input.tags.len() * 64);
        for tag in input.tags.drain(..) {
            name.extend_from_slice(b".__");
            name.extend(tag.name);
            name.extend_from_slice(b"=");
            name.extend(tag.value);
        }
        Owned {
            name: name,
            mtype: input.mtype,
            value: input.value,
            sample_rate: input.sample_rate,
            tags: vec![],
        }
    }
}

fn parse_tags(input: &[u8]) -> Result<Vec<Tag>, ParseError> {
    match input.len() {
        len if len == 0 => return Ok(vec![]),
        _ => (),
    };

    let mut tags: Vec<Tag> = Vec::new();
    let mut scan = input;
    loop {
        let tag_index_end = match memchr(b',', scan) {
            None => scan.len(),
            Some(i) => i,
        };
        let tag_scan = &scan[0..tag_index_end];
        match memchr(b':', tag_scan) {
            // Value-less tag, consume the name and continue
            None => tags.push(Tag {
                name: tag_scan.to_vec(),
                value: vec![],
            }),
            Some(value_start) => tags.push(Tag {
                name: tag_scan[0..value_start].to_vec(),
                value: tag_scan[value_start + 1..].to_vec(),
            }),
        }
        if tag_index_end == scan.len() {
            return Ok(tags);
        }
        scan = &scan[tag_index_end + 1..];
    }
}

/// Protocol Data Unit of a statsd message, with byte range accessors
///
/// Incoming protocol unit for statsd messages, commonly a single datagram or a
/// line-delimitated message. This PDU type owns an incoming message and can
/// offer references to protocol fields. It only performs limited parsing of the
/// protocol unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PDU {
    underlying: Bytes,
    value_index: usize,
    type_index: usize,
    type_index_end: usize,
    sample_rate_index: Option<(usize, usize)>,
    tags_index: Option<(usize, usize)>,
}

impl PDU {
    pub fn name(&self) -> &[u8] {
        &self.underlying[0..self.value_index - 1]
    }

    pub fn value(&self) -> &[u8] {
        &self.underlying[self.value_index..self.type_index - 1]
    }

    pub fn pdu_type(&self) -> &[u8] {
        &self.underlying[self.type_index..self.type_index_end]
    }

    pub fn tags(&self) -> Option<&[u8]> {
        self.tags_index.map(|v| &self.underlying[v.0..v.1])
    }

    pub fn sample_rate(&self) -> Option<&[u8]> {
        self.sample_rate_index.map(|v| &self.underlying[v.0..v.1])
    }

    pub fn len(&self) -> usize {
        self.underlying.len()
    }

    pub fn as_ref(&self) -> &[u8] {
        self.underlying.as_ref()
    }

    /// Return a clone of the PDU with a prefix and suffix attached to the statsd name
    pub fn with_prefix_suffix(&self, prefix: &[u8], suffix: &[u8]) -> Self {
        let offset = suffix.len() + prefix.len();

        let mut buf = bytes::BytesMut::with_capacity(self.len() + offset);
        buf.put(prefix);
        buf.put(self.name());
        buf.put(suffix);
        buf.put(self.underlying[self.value_index - 1..].as_ref());

        PDU {
            underlying: buf.freeze(),
            value_index: self.value_index + offset,
            type_index: self.type_index + offset,
            type_index_end: self.type_index_end + offset,
            sample_rate_index: self
                .sample_rate_index
                .map(|(b, e)| (b + offset, e + offset)),
            tags_index: self.tags_index.map(|(b, e)| (b + offset, e + offset)),
        }
    }

    /// Parse an incoming single protocol unit and capture internal field
    /// offsets for the positions and lengths of various protocol fields for
    /// later access. No parsing or validation of values is done, so at a low
    /// level this can be used to pass through unknown types and protocols.
    pub fn parse(line: Bytes) -> Result<Self, ParseError> {
        let length = line.len();
        let mut value_index: usize = 0;
        // To support inner ':' symbols in a metric name (more common than you
        // think) we'll first find the index of the first type separator, and
        // then do a walk to find the last ':' symbol before that.
        let type_index = memchr('|' as u8, &line).ok_or(ParseError::InvalidLine)? + 1;

        loop {
            let value_check_index = memchr(':' as u8, &line[value_index..type_index]);
            match (value_check_index, value_index) {
                (None, x) if x <= 0 => return Err(ParseError::InvalidType),
                (None, _) => break,
                _ => (),
            }
            value_index = value_check_index.unwrap() + value_index + 1;
        }
        let mut type_index_end = length;
        let mut sample_rate_index: Option<(usize, usize)> = None;
        let mut tags_index: Option<(usize, usize)> = None;

        let mut scan_index = type_index;
        loop {
            let index = memchr('|' as u8, &line[scan_index..]).map(|v| v + scan_index);
            match index {
                None => break,
                Some(x) if x + 2 >= length => break,
                Some(x) if x < type_index_end => type_index_end = x,
                _ => (),
            }
            match line[index.unwrap() + 1] {
                b'@' => {
                    if sample_rate_index.is_some() {
                        return Err(ParseError::RepeatedSampleRate);
                    }
                    sample_rate_index = index.map(|v| (v + 2, length));
                    tags_index = tags_index.map(|(v, _l)| (v, index.unwrap()));
                }
                b'#' => {
                    if tags_index.is_some() {
                        return Err(ParseError::RepeatedTags);
                    }
                    tags_index = index.map(|v| (v + 2, length));
                    sample_rate_index = sample_rate_index.map(|(v, _l)| (v, index.unwrap()));
                }
                _ => return Err(ParseError::UnsupportedExtensionField),
            }
            scan_index = index.unwrap() + 1;
        }
        Ok(PDU {
            underlying: line,
            value_index,
            type_index,
            type_index_end,
            sample_rate_index: sample_rate_index,
            tags_index: tags_index,
        })
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn parse_pdus() -> anyhow::Result<()> {
        let valid: Vec<Vec<u8>> = vec![
            b"foo.bar:3|c".to_vec(),
            b"car:bar:3|c".to_vec(),
            b"hello.bar:4.0|ms|#tags".to_vec(),
            b"hello.bar:4.0|ms|@1.0|#tags".to_vec(),
        ];
        for buf in valid {
            println!("{}", String::from_utf8(buf.clone())?);
            PDU::parse(buf.into())?;
        }
        Ok(())
    }

    #[test]
    fn simple_pdu() {
        let pdu = PDU::parse(Bytes::from_static(b"foo.car:bar:3.0|c")).unwrap();
        assert_eq!(pdu.name(), b"foo.car:bar");
        assert_eq!(pdu.value(), b"3.0");
        assert_eq!(pdu.pdu_type(), b"c")
    }

    #[test]
    fn tagged_pdu() {
        let pdu = PDU::parse(Bytes::from_static(b"foo.bar:3|c|@1.0|#tags")).unwrap();
        assert_eq!(pdu.name(), b"foo.bar");
        assert_eq!(pdu.value(), b"3");
        assert_eq!(pdu.pdu_type(), b"c");
        assert_eq!(pdu.tags().unwrap(), b"tags");
        assert_eq!(pdu.sample_rate().unwrap(), b"1.0");
    }

    #[test]
    fn tagged_pdu_reverse() {
        let pdu = PDU::parse(Bytes::from_static(b"foo.bar:3|c|#tags|@1.0")).unwrap();
        assert_eq!(pdu.name(), b"foo.bar");
        assert_eq!(pdu.value(), b"3");
        assert_eq!(pdu.pdu_type(), b"c");
        assert_eq!(pdu.tags().unwrap(), b"tags");
        assert_eq!(pdu.sample_rate().unwrap(), b"1.0");
    }

    #[test]
    fn prefix_suffix_test() {
        let opdu = PDU::parse(Bytes::from_static(b"foo.bar:3|c|#tags|@1.0")).unwrap();
        let pdu = opdu.with_prefix_suffix(b"aa", b"bbb");
        assert_eq!(pdu.name(), b"aafoo.barbbb");
        assert_eq!(pdu.value(), b"3");
        assert_eq!(pdu.pdu_type(), b"c");
        assert_eq!(pdu.tags().unwrap(), b"tags");
        assert_eq!(pdu.sample_rate().unwrap(), b"1.0");
    }

    #[test]
    fn test_parse_tag() {
        let tag_v = b"name:value";
        let r = parse_tags(tag_v).unwrap();
        assert!(r.len() == 1);
        assert_eq!(r[0].name, b"name");
        assert_eq!(r[0].value, b"value");
    }

    #[test]
    fn test_parse_tag_naked_single() {
        let tag_v = b"name";
        let r = parse_tags(tag_v).unwrap();
        assert_eq!(r[0].name, b"name");
        assert_eq!(r[0].value, b"");
    }

    #[test]
    fn test_parse_tag_complex_name() {
        let tag_v = b"name:value:value:value,name2:value2:value2:value2";
        let r = parse_tags(tag_v).unwrap();
        assert!(r.len() == 2);
        assert_eq!(r[0].name, b"name");
        assert_eq!(r[0].value, b"value:value:value");
        assert_eq!(r[1].name, b"name2");
        assert_eq!(r[1].value, b"value2:value2:value2");
    }

    #[test]
    fn test_parse_tag_none() {
        let tag_v = b"";
        let r = parse_tags(tag_v).unwrap();
        assert!(r.len() == 0);
    }

    #[test]
    fn test_parse_tag_multiple() {
        let tag_v = b"name:value,name2:value2,name3:value3";
        let r = parse_tags(tag_v).unwrap();
        assert!(r.len() == 3);
        assert_eq!(r[0].name, b"name");
        assert_eq!(r[0].value, b"value");
        assert_eq!(r[1].name, b"name2");
        assert_eq!(r[1].value, b"value2");
        assert_eq!(r[2].name, b"name3");
        assert_eq!(r[2].value, b"value3");
    }

    #[test]
    fn test_parse_tag_multiple_short() {
        let tag_v = b"name:value,name2,name3:value3";
        let r = parse_tags(tag_v).unwrap();
        assert!(r.len() == 3);
        assert_eq!(r[0].name, b"name");
        assert_eq!(r[0].value, b"value");
        assert_eq!(r[1].name, b"name2");
        assert_eq!(r[1].value, b"");
        assert_eq!(r[2].name, b"name3");
        assert_eq!(r[2].value, b"value3");
    }

    #[test]
    fn parsed_simple() {
        let pdu = PDU::parse(Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0")).unwrap();
        let parsed: Owned = (&pdu).try_into().unwrap();
        assert_eq!(parsed.value, 3.0);
        assert_eq!(parsed.name, b"foo.bar");
        assert_eq!(parsed.mtype, Type::Counter);
        assert_eq!(parsed.sample_rate, Some(1.0));
        assert_eq!(
            parsed.tags[0],
            Tag {
                name: b"tags".to_vec(),
                value: b"value".to_vec()
            }
        );
    }

    #[test]
    fn convert_roundtrip() {
        let pdu = PDU::parse(Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0")).unwrap();
        let parsed: Owned = (&pdu).try_into().unwrap();
        let pdu2: PDU = (&parsed).into();
        let parsed2: Owned = (&pdu2).try_into().unwrap();
        assert_eq!(parsed, parsed2);
    }

    pub mod convert {
        use super::*;

        #[test]
        fn convert_tags() {
            let pdu = PDU::parse(Bytes::from_static(
                b"foo.bar:3|c|#tags:value,atag:avalue|@1.0",
            ))
            .unwrap();
            let parsed = (&pdu).try_into().unwrap();
            let converted = super::super::convert::to_inline_tags(parsed);
            assert_eq!(
                b"foo.bar.__atag=avalue.__tags=value".to_vec(),
                converted.name
            );
        }
    }
}
