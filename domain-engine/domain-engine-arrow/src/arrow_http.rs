//! arrow streaming over HTTP

use std::io::Read;

use arrow_array::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter, MessageHeader};
use bytes::{BufMut, Bytes, BytesMut};
use bytes_deque::BytesDeque;
use domain_engine_core::{DomainError, DomainResult};
use flatbuffers::InvalidFlatbuffer;
use futures_util::Stream;
use tracing::{info, trace};

use crate::ArrowRespMessage;

/// https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.stream
pub const MIME: &str = "application/vnd.apache.arrow.stream";

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
                    let bytes = encoder.encode_next_batch(batch)?;

                    trace!("sending arrow batch of len={}", bytes.len());
                    yield bytes;
                }
            }
        }
    }
}

#[derive(Default)]
struct StreamEncoder {
    arrow_stream_writer: Option<StreamWriter<bytes::buf::Writer<BytesMut>>>,
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

/// Decode HTTP stream into ArrowRespMessage stream.
/// The stream error type must be mapped to DomainError before this conversion.
///
/// This is more complex because HTTP technology outside our control may
/// re-chunk the nice original RecordBatch chunks into smaller chunks,
/// so here we have to try to reassemble the original stream by analyzing
/// the actual bytes in the stream to find out where each message starts and ends,
/// or whether the stream contains a complete/incomplete messages.
pub fn http_stream_to_resp_msg_stream(
    http_stream: impl Stream<Item = DomainResult<Bytes>>,
) -> impl Stream<Item = DomainResult<ArrowRespMessage>> {
    let mut decoder = StreamDecoder::default();

    async_stream::try_stream! {
        let mut end_of_stream = false;

        for await bytes_result in http_stream {
            let bytes = bytes_result?;
            println!("received batch len={}", bytes.len());

            if !decoder.push_bytes(bytes)? {
                // end of stream
                break;
            }

            loop {
                match decoder.try_decode_next_record_batch()? {
                    ReadAttempt::Complete(batch) => {
                        yield ArrowRespMessage::RecordBatch(batch);
                    }
                    ReadAttempt::Incomplete => {
                        // wait for more bytes from http_stream
                        break;
                    }
                    ReadAttempt::EndOfStream => {
                        end_of_stream = true;
                        break;
                    }
                }
            }

            if end_of_stream {
                break;
            }
        }
    }
}

#[derive(Default)]
struct StreamDecoder {
    bytes_deque: bytes_deque::BytesDeque,
    stream_reader: Option<StreamReader<bytes_deque::BytesDeque>>,
}

enum ReadAttempt<T> {
    Complete(T),
    Incomplete,
    EndOfStream,
}

impl StreamDecoder {
    fn push_bytes(&mut self, bytes: Bytes) -> DomainResult<bool> {
        self.bytes_deque.push_back(bytes);

        // try to initialize stream reader
        if self.stream_reader.is_none() {
            // schema must be present in the stream before stream reader can be initialized
            match self.peek_schema()? {
                ReadAttempt::Complete(()) => {
                    let deque = std::mem::take(&mut self.bytes_deque);
                    let mut stream_reader = StreamReader::try_new(deque, None)
                        .map_err(|err| DomainError::protocol(format!("{err:?}")))?;
                    self.bytes_deque = std::mem::take(stream_reader.get_mut());
                    self.stream_reader = Some(stream_reader);

                    Ok(true)
                }
                ReadAttempt::Incomplete => Ok(true),
                ReadAttempt::EndOfStream => Ok(false),
            }
        } else {
            Ok(true)
        }
    }

    fn try_decode_next_record_batch(&mut self) -> DomainResult<ReadAttempt<RecordBatch>> {
        if self.stream_reader.is_none() {
            return Ok(ReadAttempt::Incomplete);
        }

        // first do a "peek" operation to see if a complete RecordBatch is potentially ready
        match self.peek_full_record_batch()? {
            ReadAttempt::Complete(()) => {}
            ReadAttempt::Incomplete => return Ok(ReadAttempt::Incomplete),
            ReadAttempt::EndOfStream => return Ok(ReadAttempt::EndOfStream),
        }

        let stream_reader = self.stream_reader.as_mut().unwrap();

        // Swap the current bytes deque into the actual message reader
        // so a message can be consumed for real
        let deque = std::mem::take(&mut self.bytes_deque);
        (*stream_reader.get_mut()) = deque;

        // consume actual message
        let next = stream_reader.next();

        // swap back
        self.bytes_deque = std::mem::take(stream_reader.get_mut());

        match next {
            Some(Ok(batch)) => Ok(ReadAttempt::Complete(batch)),
            Some(Err(err)) => Err(DomainError::protocol(format!("{err:?}"))),
            None => Err(DomainError::protocol("should have complete batch")),
        }
    }

    fn peek_schema(&self) -> DomainResult<ReadAttempt<()>> {
        let mut peekable = self.peekable_reader();

        match Self::try_read_message(&mut peekable)? {
            ReadAttempt::Complete(MessageHeader::Schema) => Ok(ReadAttempt::Complete(())),
            ReadAttempt::Complete(_) => Err(DomainError::protocol("no schema header")),
            ReadAttempt::Incomplete => Ok(ReadAttempt::Incomplete),
            ReadAttempt::EndOfStream => Ok(ReadAttempt::EndOfStream),
        }
    }

    fn peek_full_record_batch(&self) -> DomainResult<ReadAttempt<()>> {
        let mut peekable = self.peekable_reader();

        loop {
            match Self::try_read_message(&mut peekable)? {
                ReadAttempt::Complete(MessageHeader::RecordBatch) => {
                    return Ok(ReadAttempt::Complete(()))
                }
                ReadAttempt::Complete(_) => {
                    // any other message type is to be interpreted as an "argument" to a RecordBatch,
                    // so we should keep looking until RecordBatch is found
                    continue;
                }
                ReadAttempt::Incomplete => return Ok(ReadAttempt::Incomplete),
                ReadAttempt::EndOfStream => return Ok(ReadAttempt::EndOfStream),
            }
        }
    }

    /// Get a IO reader that does not consume the actual bytes.
    /// This is necessary for checking whether a message is complete before actully parsing it
    fn peekable_reader(&self) -> BytesDeque {
        // Cloning the BytesDeque is relatively cheap because Bytes::clone() is only a shallow clone
        self.bytes_deque.clone()
    }

    /// try to consume one Arrow IPC message
    fn try_read_message(
        reader: &mut bytes_deque::BytesDeque,
    ) -> DomainResult<ReadAttempt<MessageHeader>> {
        let mut meta_size_buf: [u8; 4] = [0; 4];

        if reader.read_exact(&mut meta_size_buf).is_err() {
            return Ok(ReadAttempt::Incomplete);
        }

        // all [255; 4] is a CONTINUATION_MARKER
        if meta_size_buf == [255, 255, 255, 255] && reader.read_exact(&mut meta_size_buf).is_err() {
            return Ok(ReadAttempt::Incomplete);
        }

        let Ok(meta_size) = usize::try_from(i32::from_le_bytes(meta_size_buf)) else {
            return Err(DomainError::protocol("arrow invalid meta len"));
        };

        if meta_size == 0 {
            return Ok(ReadAttempt::EndOfStream);
        }

        const HEADER_CHUNK_SIZE: usize = 64;
        // repeatedly read chunks into this buffer until a msg_type can be read
        let mut header_buf: Vec<u8> = vec![];

        let (msg_type, body_size) = loop {
            let prev_len = header_buf.len();
            header_buf.extend([0; HEADER_CHUNK_SIZE]);

            let chunk_len = reader
                .read(&mut header_buf[prev_len..prev_len + HEADER_CHUNK_SIZE])
                .map_err(|_| DomainError::protocol("could not read message header"))?;

            match flatbuffers::root::<msg_header_reader::MsgHeaderReader>(
                &header_buf[0..prev_len + chunk_len],
            ) {
                Ok(reader) => break (reader.header_type(), reader.body_length()),
                Err(InvalidFlatbuffer::RangeOutOfBounds { .. }) => {
                    if chunk_len < HEADER_CHUNK_SIZE {
                        // not enough data to read the message header
                        return Ok(ReadAttempt::Incomplete);
                    } else {
                        // read the exact chunk size, try to extend chunk by doing another iteration:
                        continue;
                    }
                }
                Err(err) => {
                    info!("arrow flatbuffers error: {err:?}");
                    return Err(DomainError::protocol("arrow IPC flatbuffers error"));
                }
            }
        };

        let Ok(body_size) = usize::try_from(body_size) else {
            return Err(DomainError::protocol("arrow invalid body length"));
        };

        let additional_bytes_needed = meta_size + body_size - header_buf.len();

        // trace!(
        //     "    try_read meta_len={meta_size}, header_buf_len={hbl}, reader_total_size={rtl}, additional_bytes_needed={additional_bytes_needed}",
        //     rtl = reader.size(),
        //     hbl = header_buf.len()
        // );

        if !reader.contains_minimum(additional_bytes_needed) {
            return Ok(ReadAttempt::Incomplete);
        }

        Ok(ReadAttempt::Complete(msg_type))
    }
}

mod msg_header_reader {
    use arrow_ipc::{Message, MessageHeader};

    /// a `flatbuffers` reader for arrow IPC messages that allows just reading the headers, not the body,
    /// so that it can work with an incomplete buffer.
    pub struct MsgHeaderReader<'a> {
        _tab: flatbuffers::Table<'a>,
    }

    impl<'a> MsgHeaderReader<'a> {
        pub fn header_type(&self) -> MessageHeader {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<MessageHeader>(Message::VT_HEADER_TYPE, Some(MessageHeader::NONE))
                    .unwrap()
            }
        }

        #[inline]
        pub fn body_length(&self) -> i64 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<i64>(Message::VT_BODYLENGTH, Some(0))
                    .unwrap()
            }
        }
    }

    impl flatbuffers::Verifiable for MsgHeaderReader<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            v.visit_table(pos)?
                .visit_field::<MessageHeader>("header_type", Message::VT_HEADER_TYPE, true)?
                .visit_field::<i64>("bodyLength", Message::VT_BODYLENGTH, false)?
                .finish();
            Ok(())
        }
    }

    impl<'a> flatbuffers::Follow<'a> for MsgHeaderReader<'a> {
        type Inner = MsgHeaderReader<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    #[test]
    fn test_read_schema() {
        let buf: [u8; 64] = [
            16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 10, 0, 9, 0, 4, 0, 10, 0, 0, 0, 16, 0, 0, 0, 0, 1, 4,
            0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 16, 0, 20,
            0, 16, 0, 0, 0, 15, 0, 4, 0,
        ];

        let reader = flatbuffers::root::<MsgHeaderReader>(&buf).unwrap();
        assert_eq!(reader.header_type(), MessageHeader::Schema);
    }

    #[test]
    fn test_read_record_batch() {
        let buf: [u8; 64] = [
            16, 0, 0, 0, 12, 0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0, 32, 0, 0, 0, 192, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0, 20, 0, 12, 0, 8, 0, 4, 0, 10, 0,
            0, 0, 36, 0, 0, 0, 12, 0, 0, 0,
        ];

        let reader = flatbuffers::root::<MsgHeaderReader>(&buf).unwrap();
        assert_eq!(reader.header_type(), MessageHeader::RecordBatch);
    }
}

mod bytes_deque {
    use std::collections::VecDeque;

    use bytes::{Buf, Bytes};

    /// A readable concatenation of Bytes stored in a ringbuffer
    #[derive(Clone, Default)]
    pub struct BytesDeque {
        deque: VecDeque<Bytes>,
    }

    impl BytesDeque {
        pub fn push_back(&mut self, bytes: Bytes) {
            self.deque.push_back(bytes);
        }

        #[allow(unused)]
        pub fn size(&self) -> usize {
            let mut len = 0;
            for bytes in &self.deque {
                len += bytes.len();
            }
            len
        }

        /// check whether this deque contains at least `capacity` unread bytes
        pub fn contains_minimum(&self, capacity: usize) -> bool {
            let mut cap = 0;

            for bytes in &self.deque {
                cap += bytes.remaining();

                if cap >= capacity {
                    return true;
                }
            }

            cap >= capacity
        }
    }

    impl std::io::Read for BytesDeque {
        fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
            let mut read = 0;

            while !buf.is_empty() {
                let Some(bytes) = self.deque.front_mut() else {
                    return Ok(read);
                };

                let bytes_remaining = bytes.remaining();

                let len = usize::min(bytes_remaining, buf.len());
                let mut start_reader = bytes.split_to(len).reader();

                std::io::copy(&mut start_reader, &mut buf)?;
                read += len;

                if bytes.is_empty() {
                    self.deque.pop_front();
                }
            }

            Ok(read)
        }
    }

    #[test]
    fn test_bytes_deque() {
        use std::io::Read;

        let mut d = BytesDeque {
            deque: Default::default(),
        };

        d.deque.push_back(Bytes::from_static(b"abc"));
        d.deque.push_back(Bytes::from_static(b"def"));
        d.deque.push_back(Bytes::from_static(b"g"));

        let mut dest: [u8; 2] = [0, 0];

        assert_eq!(2, d.read(&mut dest).unwrap());
        assert_eq!(b"ab", &dest);
        assert_eq!(3, d.deque.len());

        assert_eq!(2, d.read(&mut dest).unwrap());
        assert_eq!(b"cd", &dest);
        assert_eq!(2, d.deque.len());

        assert_eq!(2, d.read(&mut dest).unwrap());
        assert_eq!(b"ef", &dest);
        assert_eq!(1, d.deque.len());

        assert_eq!(1, d.read(&mut dest).unwrap());
        assert_eq!(b"gf", &dest);
        assert_eq!(0, d.deque.len());
    }
}
