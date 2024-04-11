use ontol_runtime::interface::serde::operator::{SequenceRange, SerdeOperatorAddr};
use thin_vec::ThinVec;

#[derive(Default)]
pub struct SequenceRangeBuilder {
    ranges: ThinVec<SequenceRange>,
}

impl SequenceRangeBuilder {
    pub fn build(self) -> ThinVec<SequenceRange> {
        self.ranges
    }

    pub fn push_required_operator(&mut self, addr: SerdeOperatorAddr) {
        match self.ranges.last_mut() {
            Some(range) => {
                if addr == range.addr {
                    // two or more identical operator addrs in row;
                    // just increase repetition counter:
                    let finite_repetition = range.finite_repetition.unwrap();
                    range.finite_repetition = Some(finite_repetition + 1);
                } else {
                    self.ranges.push(SequenceRange {
                        addr,
                        finite_repetition: Some(1),
                    });
                }
            }
            None => {
                self.ranges.push(SequenceRange {
                    addr,
                    finite_repetition: Some(1),
                });
            }
        }
    }

    pub fn push_infinite_operator(&mut self, addr: SerdeOperatorAddr) {
        self.ranges.push(SequenceRange {
            addr,
            finite_repetition: None,
        })
    }
}
