use bytes::BufMut;
use bytes::Bytes;
use memchr::memchr;
use thiserror::Error;

use std::{
    cmp::Ordering,
    convert::{TryFrom, TryInto},
    fmt,
    hash::Hash,
    hash::Hasher,
    vec,
};

/// An Owned identifier for a statsd message
#[derive(Debug, Clone, Eq)]
pub struct Id {
    pub name: Vec<u8>,
    pub mtype: Type,
    pub tags: Vec<Tag>,
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}",
            std::str::from_utf8(self.name.as_ref()).map_err(|_| fmt::Error {})?,
            self.mtype,
        )?;
        f.write_str("{")?;
        let mut sep = "";
        for tag in self.tags.iter() {
            write!(f, "{}{}", sep, tag)?;
            sep = ", ";
        }
        f.write_str("}")
    }
}

impl Hash for Id {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        for tag in self.tags.iter() {
            tag.hash(state);
        }
        self.tags.hash(state);
        self.mtype.hash(state);
    }
}

impl PartialEq for Id {
    fn eq(&self, other: &Id) -> bool {
        self.name == other.name && self.mtype == other.mtype && self.tags == other.tags
    }
}

/// The type of a statsd line or metric. The common types are covered, including
/// a few extensions such as Set and DirectGauge.
#[derive(Debug, Clone, Copy, Eq)]
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
        if value.is_empty() && value.len() > 2 {
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

impl From<&Type> for &[u8] {
    fn from(input: &Type) -> Self {
        match input {
            Type::Counter => b"c",
            Type::DirectGauge => b"G",
            Type::Gauge => b"g",
            Type::Timer => b"ms",
            Type::Set => b"s",
        }
    }
}

impl Hash for Type {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let key: &[u8] = self.into();
        key.hash(state);
    }
}

impl PartialEq for Type {
    fn eq(&self, other: &Type) -> bool {
        use Type::*;
        matches!(
            (self, other),
            (Counter, Counter)
                | (Timer, Timer)
                | (Gauge, Gauge)
                | (DirectGauge, DirectGauge)
                | (Set, Set)
        )
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Type::*;

        write!(
            f,
            "({})",
            match self {
                Counter => "counter",
                Timer => "timer",
                Gauge => "gauge",
                DirectGauge => "directgauge",
                Set => "set",
            }
        )
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
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}={}]",
            std::str::from_utf8(self.name.as_ref()).map_err(|_| fmt::Error {})?,
            std::str::from_utf8(self.value.as_ref()).map_err(|_| fmt::Error {})?
        )
    }
}

impl Hash for Tag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.value.hash(state);
    }
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
        self.name == other.name
    }
}

/// An either type representing one of the two forms of statsd protocols
///
/// In order to allow backends to operate on different levels of protocol
/// decoding (fully decoded or just tokenized), backends take a Sample enum
/// which represent either of the two formats, with easy conversions between
/// them.
///
/// # Examples
/// ```
/// use statsrelay::statsd_proto;
/// use bytes::Bytes;
/// use statsrelay::statsd_proto::Event;
///
/// let input = Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0");
/// let sample = &Event::Pdu(statsd_proto::Pdu::parse(input).unwrap());
/// let parsed: statsd_proto::Pdu = sample.into();
/// ```
#[derive(Clone, Debug)]
pub enum Event {
    Pdu(Pdu),
    Parsed(Owned),
}

impl Hash for Event {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Event::Pdu(pdu) => pdu.hash(state),
            Event::Parsed(parsed) => parsed.hash(state),
        }
    }
}

impl TryFrom<&Event> for Owned {
    type Error = ParseError;
    fn try_from(inp: &Event) -> Result<Self, Self::Error> {
        match inp {
            Event::Parsed(p) => Ok(p.to_owned()),
            Event::Pdu(pdu) => pdu.try_into(),
        }
    }
}

impl TryFrom<Event> for Owned {
    type Error = ParseError;
    fn try_from(inp: Event) -> Result<Self, Self::Error> {
        match inp {
            Event::Parsed(p) => Ok(p),
            Event::Pdu(pdu) => pdu.try_into(),
        }
    }
}

impl From<Event> for Pdu {
    fn from(inp: Event) -> Self {
        match inp {
            Event::Pdu(pdu) => pdu,
            Event::Parsed(p) => p.into(),
        }
    }
}

impl From<&Event> for Pdu {
    fn from(inp: &Event) -> Self {
        match inp {
            Event::Pdu(pdu) => pdu.clone(),
            Event::Parsed(p) => p.into(),
        }
    }
}

pub trait Parsed {
    fn id(&self) -> &Id;
    fn name(&self) -> &[u8];
    fn metric_type(&self) -> &Type;
    fn value(&self) -> f64;
    fn sample_rate(&self) -> Option<f64>;
    fn tags(&self) -> &[Tag];
}

/// A structured and owned version of [`PDU`](PDU)
///
/// Gives an owned structure which represents a parsed statsd protocol unit
/// which owns all of its fields. When parsing, no canonicalization is performed
/// by default.
#[derive(Debug, Clone)]
pub struct Owned {
    id: Id,
    value: f64,
    sample_rate: Option<f64>,
}

impl Hash for Owned {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Owned {
    fn eq(&self, other: &Owned) -> bool {
        self.id.eq(&other.id) && self.value == other.value && self.sample_rate == other.sample_rate
    }
}

impl Owned {
    pub fn new(id: Id, value: f64, sample_rate: Option<f64>) -> Self {
        Owned {
            id,
            value,
            sample_rate,
        }
    }
}

impl Parsed for Owned {
    fn id(&self) -> &Id {
        &self.id
    }
    fn name(&self) -> &[u8] {
        self.id.name.as_ref()
    }
    fn metric_type(&self) -> &Type {
        &self.id.mtype
    }
    fn value(&self) -> f64 {
        self.value
    }
    fn sample_rate(&self) -> Option<f64> {
        self.sample_rate
    }
    fn tags(&self) -> &[Tag] {
        self.id.tags.as_slice()
    }
}

impl TryFrom<Pdu> for Owned {
    type Error = ParseError;

    fn try_from(value: Pdu) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&Pdu> for Owned {
    type Error = ParseError;

    fn try_from(pdu: &Pdu) -> Result<Self, Self::Error> {
        let value = match lexical::parse::<f64, _>(pdu.value()) {
            Ok(v) if v.is_finite() => v,
            _ => return Err(ParseError::InvalidValue),
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
        let id = Id {
            name: pdu.name().to_vec(),
            mtype,
            tags: tags.unwrap_or_default(),
        };
        Ok(Owned {
            id,
            value,
            sample_rate,
        })
    }
}

impl From<Owned> for Pdu {
    fn from(input: Owned) -> Self {
        (&input).into()
    }
}

impl From<&Owned> for Pdu {
    fn from(input: &Owned) -> Self {
        let mut bytes = Vec::with_capacity(input.id.name.len() + (input.id.tags.len() * 64) + 64);

        bytes.extend(&input.id.name);
        bytes.push(b':');
        let value_index = bytes.len();
        bytes.extend(lexical::to_string(input.value).as_bytes());
        bytes.push(b'|');
        let type_index = bytes.len();
        let mtype = &input.id.mtype;
        bytes.extend_from_slice(mtype.into());
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

        let tags_index = if !input.id.tags.is_empty() {
            bytes.extend_from_slice(b"|#");
            let start = bytes.len();
            let mut peek = input.id.tags.iter().peekable();
            while let Some(tag) = peek.next() {
                bytes.extend(&tag.name);
                bytes.push(b':');
                bytes.extend(&tag.value);
                if peek.peek().is_some() {
                    bytes.push(b',');
                }
            }
            Some((start, bytes.len()))
        } else {
            None
        };
        Pdu {
            underlying: Bytes::from(bytes),
            value_index,
            type_index,
            type_index_end,
            sample_rate_index,
            tags_index,
        }
    }
}

pub mod convert {
    use super::*;
    /// Convert from external tags to internal tags. Does not check for
    /// collisions of existing inline tags with the newly generated inline tags.

    fn inline_sanitize<T>(input: T) -> impl Iterator<Item = u8>
    where
        T: IntoIterator<Item = u8>,
    {
        input.into_iter().map({
            |character| match character {
                b':' | b'.' | b'=' => b'_',
                _ => character,
            }
        })
    }

    pub fn to_inline_tags(mut input: Owned) -> Owned {
        if input.id.tags.is_empty() {
            return input;
        }
        input.id.tags.sort();
        let mut name = input.id.name;
        // Estimate on tag size without iterating through all actual tags
        name.reserve(input.id.tags.len() * 64);
        for tag in input.id.tags.drain(..) {
            name.extend_from_slice(b".__");
            name.extend(inline_sanitize(tag.name));
            name.extend_from_slice(b"=");
            name.extend(inline_sanitize(tag.value));
        }
        let id = Id {
            name,
            mtype: input.id.mtype,
            tags: vec![],
        };
        Owned {
            id,
            value: input.value,
            sample_rate: input.sample_rate,
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
#[derive(Debug, Clone)]
pub struct Pdu {
    underlying: Bytes,
    value_index: usize,
    type_index: usize,
    type_index_end: usize,
    sample_rate_index: Option<(usize, usize)>,
    tags_index: Option<(usize, usize)>,
}

impl Hash for Pdu {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.pdu_type().hash(state);
    }
}

impl Pdu {
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_bytes(&self) -> &[u8] {
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

        Pdu {
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
        let type_index = memchr(b'|', &line).ok_or(ParseError::InvalidLine)? + 1;

        loop {
            let value_check_index = memchr(b':', &line[value_index..type_index]);
            match (value_check_index, value_index) {
                (None, x) if x == 0 => return Err(ParseError::InvalidType),
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
            let index = memchr(b'|', &line[scan_index..]).map(|v| v + scan_index);
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
                _ => (),
            }
            scan_index = index.unwrap() + 1;
        }
        Ok(Pdu {
            underlying: line,
            value_index,
            type_index,
            type_index_end,
            sample_rate_index,
            tags_index,
        })
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use std::collections::HashMap;

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
            Pdu::parse(buf.into())?;
        }
        Ok(())
    }

    #[test]
    fn simple_pdu() {
        let pdu = Pdu::parse(Bytes::from_static(b"foo.car:bar:3.0|c")).unwrap();
        assert_eq!(pdu.name(), b"foo.car:bar");
        assert_eq!(pdu.value(), b"3.0");
        assert_eq!(pdu.pdu_type(), b"c")
    }

    #[test]
    fn tagged_pdu() {
        let pdu = Pdu::parse(Bytes::from_static(b"foo.bar:3|c|@1.0|#tags")).unwrap();
        assert_eq!(pdu.name(), b"foo.bar");
        assert_eq!(pdu.value(), b"3");
        assert_eq!(pdu.pdu_type(), b"c");
        assert_eq!(pdu.tags().unwrap(), b"tags");
        assert_eq!(pdu.sample_rate().unwrap(), b"1.0");
    }

    #[test]
    fn tagged_pdu_reverse() {
        let pdu = Pdu::parse(Bytes::from_static(b"foo.bar:3|c|#tags|@1.0")).unwrap();
        assert_eq!(pdu.name(), b"foo.bar");
        assert_eq!(pdu.value(), b"3");
        assert_eq!(pdu.pdu_type(), b"c");
        assert_eq!(pdu.tags().unwrap(), b"tags");
        assert_eq!(pdu.sample_rate().unwrap(), b"1.0");
    }

    #[test]
    fn prefix_suffix_test() {
        let opdu = Pdu::parse(Bytes::from_static(b"foo.bar:3|c|#tags|@1.0")).unwrap();
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
        assert!(r.is_empty());
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
        let pdu = Pdu::parse(Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0")).unwrap();
        let parsed: Owned = (&pdu).try_into().unwrap();
        assert_eq!(parsed.value, 3.0);
        assert_eq!(parsed.id.name, b"foo.bar");
        assert_eq!(parsed.id.mtype, Type::Counter);
        assert_eq!(parsed.sample_rate, Some(1.0));
        assert_eq!(
            parsed.id.tags[0],
            Tag {
                name: b"tags".to_vec(),
                value: b"value".to_vec()
            }
        );
    }

    #[test]
    fn parsed_tags_complex() {
        let pdu = Pdu::parse(Bytes::from_static(b"foo.bar:3|c|#tags|tagpt2:value|@1.0")).unwrap();
        let parsed: Owned = (&pdu).try_into().unwrap();
        assert_eq!(parsed.value, 3.0);
        assert_eq!(parsed.id.name, b"foo.bar");
        assert_eq!(parsed.id.mtype, Type::Counter);
        assert_eq!(parsed.sample_rate, Some(1.0));
        assert_eq!(
            parsed.id.tags[0],
            Tag {
                name: b"tags|tagpt2".to_vec(),
                value: b"value".to_vec()
            }
        );
    }

    #[test]
    fn convert_roundtrip() {
        let pdu = Pdu::parse(Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0")).unwrap();
        let parsed: Owned = (&pdu).try_into().unwrap();
        let pdu2: Pdu = (&parsed).into();
        let parsed2: Owned = (&pdu2).try_into().unwrap();
        assert_eq!(parsed, parsed2);
    }

    /// This test is designed to check that the contracts on using a Id in
    /// a hashmap are not violated for reference-accelerated lookups
    #[test]
    fn test_key_hashing_borrow() {
        let mut map: HashMap<Id, bool> = HashMap::new();
        let id1 = Id {
            name: b"hello".to_vec(),
            mtype: Type::Counter,
            tags: vec![],
        };
        map.insert(id1, true);
        // Generate an id right from a metric
        let owned: Owned = Pdu::parse(Bytes::from_static(b"hello:3|c|@1.0"))
            .unwrap()
            .try_into()
            .unwrap();

        assert_eq!(map.get(&owned.id), Some(&true));
    }

    #[test]
    fn test_fmt_id() {
        let id1 = Id {
            name: b"hello".to_vec(),
            mtype: Type::Counter,
            tags: vec![],
        };
        let fmt1 = format!("{}", id1);
        assert!("hello(counter){}" == fmt1);

        let id2 = Id {
            name: b"hello".to_vec(),
            mtype: Type::Counter,
            tags: vec![Tag {
                name: b"a".to_vec(),
                value: b"b".to_vec(),
            }],
        };
        let fmt2 = format!("{}", id2);
        assert!(
            "hello(counter){[a=b]}" == fmt2,
            "invalid format output, got {}",
            fmt2
        );
    }

    pub mod convert {
        use super::*;

        #[test]
        fn convert_tags() {
            let pdu = Pdu::parse(Bytes::from_static(
                b"foo.bar:3|c|#tags:value,atag:avalue|@1.0",
            ))
            .unwrap();
            let parsed = (&pdu).try_into().unwrap();
            let converted = super::super::convert::to_inline_tags(parsed);
            assert_eq!(
                b"foo.bar.__atag=avalue.__tags=value".to_vec(),
                converted.id.name
            );
        }

        #[test]
        fn convert_tags_dirty() {
            let pdu = Pdu::parse(Bytes::from_static(
                b"foo.bar:3|c|#tags.extra.name:value=iscool,atag:avalue:withanextracolon|@1.0",
            ))
            .unwrap();
            let parsed = (&pdu).try_into().unwrap();
            let converted = super::super::convert::to_inline_tags(parsed);
            assert_eq!(
                b"foo.bar.__atag=avalue_withanextracolon.__tags_extra_name=value_iscool".to_vec(),
                converted.id.name
            );
        }
    }
}
