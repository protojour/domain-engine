use tracing::debug;

use crate::{def::EdgeParams, error::CompileError, relation::RelationshipId};

/// A sequence represents both finite tuples and infinite arrays,
/// where elements may be of different types.
#[derive(Debug, Default)]
pub struct Sequence {
    elements: Vec<SequenceElement>,

    /// Whether the last element continues to be accepted ad infinitum.
    infinite: bool,
}

impl Sequence {
    pub fn elements(&self) -> &[SequenceElement] {
        self.elements.as_slice()
    }

    pub fn is_infinite(&self) -> bool {
        self.infinite
    }

    pub fn define_relationship(
        &mut self,
        edge_params: &EdgeParams,
        relationship_id: RelationshipId,
    ) -> Result<(), CompileError> {
        let range = match edge_params {
            EdgeParams::IndexRange(range) => range,
            _ => return Err(CompileError::UnsupportedSequenceIndexType),
        };

        debug!("define sequence relationship for range {range:?}");

        match (range.start, range.end) {
            (start, Some(end)) => {
                let start = start.unwrap_or(0);
                self.ensure_size(end as usize);
                for index in start..end {
                    self.define_element(index, relationship_id)?;
                }
                Ok(())
            }
            (Some(_), None) if self.infinite => Err(CompileError::OverlappingSequenceIndexes),
            (Some(start), None) => {
                self.ensure_size(start as usize + 1);
                self.define_element(start, relationship_id)?;
                self.infinite = true;
                Ok(())
            }
            (None, None) => Err(CompileError::UnsupportedSequenceIndexType),
        }
    }

    fn ensure_size(&mut self, size: usize) {
        while self.elements.len() < size {
            self.elements.push(SequenceElement::Undefined)
        }
    }

    fn define_element(
        &mut self,
        index: u16,
        relationship_id: RelationshipId,
    ) -> Result<(), CompileError> {
        let element = self.elements.get_mut(index as usize).unwrap();
        match element {
            SequenceElement::Defined(..) => Err(CompileError::OverlappingSequenceIndexes),
            SequenceElement::Undefined => {
                *element = SequenceElement::Defined(relationship_id);
                Ok(())
            }
        }
    }
}

#[derive(Default, Debug)]
pub enum SequenceElement {
    /// Corresponds to the Unit type:
    #[default]
    Undefined,

    Defined(RelationshipId),
}
