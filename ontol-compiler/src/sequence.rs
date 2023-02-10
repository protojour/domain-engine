use std::collections::BTreeMap;

use tracing::debug;

use crate::{def::EdgeParams, error::CompileError, relation::RelationshipId};

/// A sequence represents both finite tuples and infinite arrays,
/// where elements may be of different types.
#[derive(Debug, Default)]
pub struct Sequence {
    /// elements are stored in a map to avoid large "holes"
    /// of potentially undefined index gaps.
    ///
    /// TODO: Map should instead contain contiguous regions, i.e. Range as key,
    /// which are continuously merged together in define_relationship
    elements: BTreeMap<u16, RelationshipId>,

    /// Whether the last element continues to be accepted ad infinitum.
    infinite: bool,
}

impl Sequence {
    pub fn elements(&self) -> impl Iterator<Item = (u16, Option<RelationshipId>)> + '_ {
        let mut next_index: u16 = 0;
        let mut iterator = self.elements.iter().peekable();

        // enumerate indexes, yield Some(_) if index exists in the map.
        std::iter::from_fn(move || {
            let item = match iterator.peek() {
                None => None,
                Some((index, relationship_id)) if **index == next_index => {
                    let elem = Some((**index, Some(**relationship_id)));
                    iterator.next();
                    elem
                }
                Some(_) => Some((next_index, None)),
            };

            next_index += 1;
            item
        })
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
                for index in start..end {
                    self.define_element(index, relationship_id)?;
                }
                Ok(())
            }
            (Some(_), None) if self.infinite => Err(CompileError::OverlappingSequenceIndexes),
            (Some(start), None) => {
                self.define_element(start, relationship_id)?;
                self.infinite = true;
                Ok(())
            }
            (None, None) => Err(CompileError::UnsupportedSequenceIndexType),
        }
    }

    fn define_element(
        &mut self,
        index: u16,
        relationship_id: RelationshipId,
    ) -> Result<(), CompileError> {
        match self.elements.insert(index, relationship_id) {
            Some(_) => Err(CompileError::OverlappingSequenceIndexes),
            None => Ok(()),
        }
    }
}
