use crate::{
    interface::serde::{
        operator::{SequenceRange, SerdeOperatorAddr},
        processor::SubProcessorContext,
    },
    DefId,
};

use super::ValueMatcher;

pub struct SeqElementMatch {
    pub element_addr: SerdeOperatorAddr,
    pub ctx: SubProcessorContext,
}

#[derive(Clone)]
pub struct SequenceRangesMatcher<'on> {
    ranges: &'on [SequenceRange],
    range_cursor: usize,
    repetition_cursor: u16,

    pub type_def_id: DefId,
    pub ctx: SubProcessorContext,
}

impl<'on> ValueMatcher for SequenceRangesMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut len: usize = 0;
        let mut finite = true;
        for range in self.ranges {
            if let Some(finite_repetition) = &range.finite_repetition {
                finite = true;
                len += *finite_repetition as usize;
            } else {
                finite = false;
            }
        }

        if finite {
            write!(f, "sequence with length {len}")
        } else {
            write!(f, "sequence with minimum length {len}")
        }
    }

    fn match_sequence(&self) -> Result<SequenceRangesMatcher, ()> {
        Ok(self.clone())
    }
}

impl<'on> SequenceRangesMatcher<'on> {
    pub fn new(ranges: &'on [SequenceRange], type_def_id: DefId, ctx: SubProcessorContext) -> Self {
        Self {
            ranges,
            range_cursor: 0,
            repetition_cursor: 0,
            type_def_id,
            ctx,
        }
    }

    pub fn match_next_seq_element(&mut self) -> Option<SeqElementMatch> {
        loop {
            let range = &self.ranges[self.range_cursor];

            if let Some(finite_repetition) = range.finite_repetition {
                if self.repetition_cursor < finite_repetition {
                    self.repetition_cursor += 1;

                    return Some(SeqElementMatch {
                        element_addr: range.addr,
                        ctx: self.ctx,
                    });
                } else {
                    self.range_cursor += 1;
                    self.repetition_cursor = 0;

                    if self.range_cursor == self.ranges.len() {
                        return None;
                    } else {
                        continue;
                    }
                }
            } else {
                return Some(SeqElementMatch {
                    element_addr: range.addr,
                    ctx: self.ctx,
                });
            }
        }
    }

    pub fn match_seq_end(&self) -> Result<(), ()> {
        if self.range_cursor == self.ranges.len() {
            return Ok(());
        } else if !self.ranges.is_empty() && self.range_cursor < self.ranges.len() - 1 {
            return Err(());
        }

        let range = &self.ranges[self.range_cursor];
        if let Some(finite_repetition) = range.finite_repetition {
            // note: repetition cursor has already been increased in match_next_seq_element
            if self.repetition_cursor - 1 < finite_repetition {
                Err(())
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct IndexSetMatcher<'on> {
    range: &'on SequenceRange,

    pub type_def_id: DefId,
    pub ctx: SubProcessorContext,
}

impl<'on> IndexSetMatcher<'on> {
    pub fn new(range: &'on SequenceRange, type_def_id: DefId, ctx: SubProcessorContext) -> Self {
        Self {
            range,
            type_def_id,
            ctx,
        }
    }
}
