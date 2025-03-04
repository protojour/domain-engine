use tantivy::schema::{
    FAST, FacetOptions, Field, IndexRecordOption, JsonObjectOptions, Schema, TextFieldIndexing,
    TextOptions,
};

pub mod fieldname {
    pub const VERTEX_ADDR: &str = "vertex_addr";
    pub const FACET: &str = "facet";
    pub const UPDATE_TIME: &str = "update_time";
    pub const TEXT: &str = "text";
    pub const DATA: &str = "data";
}

use crate::TOKENIZER_NAME;

#[derive(Clone)]
pub struct SchemaWithMeta {
    pub schema: Schema,
    pub vertex_addr: Field,
    pub facet: Field,
    pub update_time: Field,
    pub text: Field,
    pub data: Field,
}

pub fn make_schema() -> SchemaWithMeta {
    let mut builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER_NAME)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);

    let vertex_addr = builder.add_bytes_field(fieldname::VERTEX_ADDR, FAST);
    let facet = builder.add_facet_field(fieldname::FACET, FacetOptions::default());
    let update_time = builder.add_date_field(fieldname::UPDATE_TIME, FAST);
    let text = builder.add_text_field(
        fieldname::TEXT,
        TextOptions::default().set_indexing_options(text_field_indexing.clone()),
    );
    let data = builder.add_json_field(
        fieldname::DATA,
        JsonObjectOptions::default().set_indexing_options(text_field_indexing),
    );

    SchemaWithMeta {
        schema: builder.build(),
        vertex_addr,
        facet,
        update_time,
        text,
        data,
    }
}
