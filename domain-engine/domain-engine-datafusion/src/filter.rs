use std::collections::BTreeMap;

use datafusion::prelude::Expr;
use domain_engine_core::domain_select;
use ontol_runtime::{
    ontology::Ontology,
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    DefId, PropId,
};

use crate::arrow::{iter_arrow_fields, FieldType};

#[derive(Clone)]
pub struct DatafusionFilter {
    entity_select: EntitySelect,
    column_selection: Vec<(PropId, FieldType)>,
}

impl DatafusionFilter {
    pub fn compile(
        def_id: DefId,
        (projection, _filters, limit): (Option<&Vec<usize>>, &[Expr], Option<usize>),
        ontology: &Ontology,
    ) -> Self {
        let def = ontology.def(def_id);
        let mut select_properties: BTreeMap<PropId, Select> = Default::default();
        let mut columns = vec![];

        let fields: Vec<_> = iter_arrow_fields(def, ontology).collect();

        if let Some(projection) = projection {
            for field_idx in projection {
                let Some(field_info) = fields.get(*field_idx) else {
                    continue;
                };

                select_properties.insert(field_info.prop_id, Select::Unit);
                columns.push((field_info.prop_id, field_info.field_type));
            }
        } else {
            if let Select::Struct(struct_select) =
                domain_select::domain_select_no_edges(def_id, ontology)
            {
                select_properties = struct_select.properties;
            }

            columns = fields
                .iter()
                .map(|field_info| (field_info.prop_id, field_info.field_type))
                .collect();
        }

        DatafusionFilter {
            entity_select: EntitySelect {
                source: StructOrUnionSelect::Struct(StructSelect {
                    def_id,
                    properties: select_properties,
                }),
                filter: Filter::default_for_domain(),
                limit,
                after_cursor: None,
                include_total_len: false,
            },
            column_selection: columns,
        }
    }

    pub fn entity_select(&self) -> EntitySelect {
        self.entity_select.clone()
    }

    pub fn column_selection(&self) -> Vec<(PropId, FieldType)> {
        self.column_selection.clone()
    }
}
