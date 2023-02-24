use ontol_runtime::serde::{SequenceRange, SerdeOperatorId};
use smallvec::SmallVec;

#[derive(Default)]
pub struct SequenceRangeBuilder {
    ranges: SmallVec<[SequenceRange; 3]>,
}

impl SequenceRangeBuilder {
    pub fn build(self) -> SmallVec<[SequenceRange; 3]> {
        self.ranges
    }

    pub fn push_required_operator(&mut self, operator_id: SerdeOperatorId) {
        match self.ranges.last_mut() {
            Some(range) => {
                if operator_id == range.operator_id {
                    // two or more identical operator ids in row;
                    // just increase repetition counter:
                    let finite_repetition = range.finite_repetition.unwrap();
                    range.finite_repetition = Some(finite_repetition + 1);
                } else {
                    self.ranges.push(SequenceRange {
                        operator_id,
                        finite_repetition: Some(1),
                    });
                }
            }
            None => {
                self.ranges.push(SequenceRange {
                    operator_id,
                    finite_repetition: Some(1),
                });
            }
        }
    }

    pub fn push_infinite_operator(&mut self, operator_id: SerdeOperatorId) {
        self.ranges.push(SequenceRange {
            operator_id,
            finite_repetition: None,
        })
    }
}
