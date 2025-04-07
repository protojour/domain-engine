use crate::span::U32Span;

#[derive(Debug)]
pub struct SpannedMsgError {
    pub msg: String,
    pub span: U32Span,
}
