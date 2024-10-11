//! arrow streaming over HTTP

use arrow_array::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use domain_engine_core::{DomainError, DomainResult};
use futures_util::Stream;

use crate::ArrowRespMessage;

/// https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.stream
pub const MIME: &str = "application/vnd.apache.arrow.stream";

#[derive(Default)]
struct StreamEncoder {
    arrow_stream_writer: Option<StreamWriter<bytes::buf::Writer<BytesMut>>>,
}

#[derive(Default)]
struct StreamDecoder {
    stream_reader: Option<StreamReader<bytes::buf::Reader<Bytes>>>,
}

/// Encode ArrowRespMessage stream for sending over HTTP chunked encoding.
pub fn resp_msg_to_http_stream(
    resp_stream: impl Stream<Item = DomainResult<ArrowRespMessage>>,
) -> impl Stream<Item = DomainResult<Bytes>> {
    // The encoder has internal state and lives for the duration of the stream
    let mut encoder = StreamEncoder::default();

    async_stream::try_stream! {
        for await result in resp_stream {
            match result? {
                ArrowRespMessage::RecordBatch(batch) => {
                    yield encoder.encode_next_batch(batch)?;
                }
            }
        }
    }
}

/// Decode HTTP stream into ArrowRespMessage stream.
/// The stream error type must be mapped to DomainError before this conversion.
pub fn http_stream_to_resp_msg_stream(
    http_stream: impl Stream<Item = DomainResult<Bytes>>,
) -> impl Stream<Item = DomainResult<ArrowRespMessage>> {
    let mut decoder = StreamDecoder::default();

    async_stream::try_stream! {
        for await bytes_result in http_stream {
            let bytes = bytes_result?;
            println!("received batch len={}", bytes.len());

            decoder.consume_bytes(bytes)?;

            println!("bytes consumed");

            while let Some(batch_result) = decoder.read_batch() {
                yield ArrowRespMessage::RecordBatch(batch_result?);
            }
        }
    }
}

impl StreamEncoder {
    fn encode_next_batch(&mut self, batch: RecordBatch) -> DomainResult<Bytes> {
        if self.arrow_stream_writer.is_none() {
            // stream writer must be initialized with a schema,
            // so it's created when the first batch comes in
            self.arrow_stream_writer = Some(
                arrow_ipc::writer::StreamWriter::try_new(
                    BytesMut::new().writer(),
                    batch.schema_ref(),
                )
                .map_err(|err| DomainError::protocol(format!("{err:?}")))?,
            );
        }

        let stream_writer = self.arrow_stream_writer.as_mut().unwrap();
        stream_writer
            .write(&batch)
            .map_err(|err| DomainError::protocol(format!("{err:?}")))?;

        Ok(
            // replace the BytesMut writer with a new one,
            // then freeze the newly written BytesMut to produce immutable Bytes
            std::mem::replace(stream_writer.get_mut(), BytesMut::new().writer())
                .into_inner()
                .freeze(),
        )
    }
}

impl StreamDecoder {
    /// Feed the decoder some bytes
    fn consume_bytes(&mut self, bytes: Bytes) -> DomainResult<()> {
        if let Some(stream_reader) = self.stream_reader.as_mut() {
            (*stream_reader.get_mut()) = bytes.reader();
        } else {
            self.stream_reader = Some(
                StreamReader::try_new(bytes.reader(), None)
                    .map_err(|err| DomainError::protocol(format!("{err:?}")))?,
            );
        }

        Ok(())
    }

    /// Read the next batch
    fn read_batch(&mut self) -> Option<DomainResult<RecordBatch>> {
        let stream_reader = self.stream_reader.as_mut().unwrap();

        println!(
            "read batch, remaining = {}",
            stream_reader.get_ref().get_ref().len()
        );

        if stream_reader.get_ref().get_ref().is_empty() {
            return None;
        }

        match stream_reader.next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => {
                println!("stream reader error: {err:?}");
                Some(Err(DomainError::protocol(format!("{err:?}"))))
            }
            None => {
                println!("stream reader next returned None");
                Some(Err(DomainError::protocol("premature end of arrow stream")))
            }
        }
    }
}
