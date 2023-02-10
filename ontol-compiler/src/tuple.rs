use crate::{def::EdgeParams, error::CompileError, relation::RelationshipId};

/// A tuple represents both finite and infinite arrays,
/// where elements may be of different types.
#[derive(Debug, Default)]
pub struct Tuple {
    elements: Vec<TupleElement>,

    /// Whether the last element continues to be accepted ad infinitum.
    infinite: bool,
}

impl Tuple {
    pub fn elements(&self) -> &[TupleElement] {
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
            _ => return Err(CompileError::UnsupportedTupleIndexType),
        };

        match (range.start, range.end) {
            (start, Some(end)) => {
                let start = start.unwrap_or(0);
                self.ensure_size(end as usize);
                for index in start..end {
                    self.define_element(index, relationship_id)?;
                }
                Ok(())
            }
            (Some(_), None) if self.infinite => Err(CompileError::OverlappingTupleIndexes),
            (Some(start), None) => {
                self.ensure_size(start as usize);
                self.define_element(start, relationship_id)?;
                self.infinite = true;
                Ok(())
            }
            (None, None) => Err(CompileError::UnsupportedTupleIndexType),
        }
    }

    fn ensure_size(&mut self, size: usize) {
        while self.elements.len() < size {
            self.elements.push(TupleElement::Undefined)
        }
    }

    fn define_element(
        &mut self,
        index: u16,
        relationship_id: RelationshipId,
    ) -> Result<(), CompileError> {
        let element = self.elements.get_mut(index as usize).unwrap();
        match element {
            TupleElement::Defined(..) => Err(CompileError::OverlappingTupleIndexes),
            TupleElement::Undefined => {
                *element = TupleElement::Defined(relationship_id);
                Ok(())
            }
        }
    }
}

#[derive(Default, Debug)]
pub enum TupleElement {
    /// Corresponds to the Unit type:
    #[default]
    Undefined,

    Defined(RelationshipId),
}
