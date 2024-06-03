//! This library will contain the gRPC integration
//!
//! For now it's just an experiment

use prost::encoding::{DecodeContext, WireType};

#[derive(Debug)]
pub struct TestMessage {
    name: String,
}

impl prost::Message for TestMessage {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: prost::bytes::BufMut,
        Self: Sized,
    {
        if !self.name.is_empty() {
            ::prost::encoding::string::encode(1, &self.name, buf);
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        B: prost::bytes::Buf,
        Self: Sized,
    {
        const STRUCT_NAME: &str = stringify!(TestMessage);
        match tag {
            1 => ::prost::encoding::string::merge(wire_type, &mut self.name, buf, ctx).map_err(
                |mut error| {
                    error.push(STRUCT_NAME, stringify!(name));
                    error
                },
            ),
            _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        if !self.name.is_empty() {
            ::prost::encoding::string::encoded_len(1u32, &self.name)
        } else {
            0
        }
    }

    fn clear(&mut self) {
        self.name.clear();
    }
}
