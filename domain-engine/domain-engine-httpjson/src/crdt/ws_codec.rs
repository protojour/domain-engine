use domain_engine_core::{domain_error::DomainErrorKind, DomainError};

/// A very scaled down version of automerge sync protocol,
/// since here there is one endpoint per document, and lots of fields can be skipped
pub enum Message {
    Join,
    Peer,
    Sync(Vec<u8>),
}

#[derive(Debug)]
pub enum DecodeError {
    MissingType,
    MissingLen,
    MissingData,
    UnknownType(String),
    Cbor(String),
}

impl Message {
    pub fn decode_cbor(buf: &[u8]) -> Result<Self, DecodeError> {
        let mut decoder = minicbor::Decoder::new(buf);

        let mut r#type: Option<&str> = None;
        let mut message: Option<Vec<u8>> = None;

        for _ in 0..decoder.map()?.ok_or(DecodeError::MissingLen)? {
            let k = decoder.str()?;
            match k {
                "type" => r#type = Some(decoder.str()?),
                "message" => {
                    message = {
                        decoder.tag()?;
                        Some(decoder.bytes()?.to_vec())
                    }
                }
                _ => decoder.skip()?,
            }
        }

        match r#type {
            None => Err(DecodeError::MissingType),
            Some("join") => Ok(Self::Join),
            Some("sync") | Some("message") => {
                Ok(Self::Sync(message.ok_or(DecodeError::MissingData)?))
            }
            Some("peer") => Ok(Self::Peer),
            Some(other) => Err(DecodeError::UnknownType(other.to_string())),
        }
    }

    pub fn encode_cbor(&self) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        let mut encoder = minicbor::Encoder::new(&mut out);
        match self {
            Message::Join => {
                encoder.map(3).unwrap();

                encoder.str("type").unwrap();
                encoder.str("join").unwrap();

                encoder.str("senderId").unwrap();
                encoder.str("").unwrap();

                encoder.str("channelId").unwrap();
                encoder.str("sync").unwrap();
            }
            Message::Peer => {
                encoder.map(2).unwrap();

                encoder.str("type").unwrap();
                encoder.str("peer").unwrap();

                encoder.str("senderId").unwrap();
                encoder.str("").unwrap();
            }
            Message::Sync(msg) => {
                encoder.map(5).unwrap();

                encoder.str("type").unwrap();
                encoder.str("message").unwrap();

                encoder.str("senderId").unwrap();
                encoder.str("").unwrap();

                encoder.str("targetId").unwrap();
                encoder.str("").unwrap();

                encoder.str("channelId").unwrap();
                encoder.str("").unwrap();

                encoder.str("message").unwrap();
                encoder
                    .tag(minicbor::data::IanaTag::TypedArrayU8.tag())
                    .unwrap();
                encoder.bytes(msg.as_slice()).unwrap();
            }
        }

        out
    }
}

impl From<minicbor::decode::Error> for DecodeError {
    fn from(value: minicbor::decode::Error) -> Self {
        Self::Cbor(value.to_string())
    }
}

impl From<DecodeError> for DomainError {
    fn from(value: DecodeError) -> Self {
        let msg = match value {
            DecodeError::MissingType => "missing type".to_string(),
            DecodeError::MissingLen => "missing len".to_string(),
            DecodeError::MissingData => "missing data".to_string(),
            DecodeError::UnknownType(t) => format!("unknown type: {t}"),
            DecodeError::Cbor(msg) => msg,
        };
        DomainErrorKind::Protocol(msg).into_error()
    }
}
