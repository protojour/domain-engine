use tantivy::schema::{
    FacetOptions, Field, IndexRecordOption, JsonObjectOptions, Schema, TextFieldIndexing,
    TextOptions, FAST,
};

use crate::TOKENIZER_NAME;

#[derive(Clone)]
pub struct SchemaWithMeta {
    pub schema: Schema,
    pub vertex_addr: Field,
    pub domain_def_id: Field,
    pub update_time: Field,
    pub text: Field,
    pub data: Field,
}

pub fn make_schema() -> SchemaWithMeta {
    let mut builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER_NAME)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);

    let vertex_addr = builder.add_bytes_field("vertex_addr", FAST);
    let domain_def_id = builder.add_facet_field("domain_def_id", FacetOptions::default());
    let update_time = builder.add_date_field("update_time", FAST);
    let text = builder.add_text_field(
        "text",
        TextOptions::default().set_indexing_options(text_field_indexing.clone()),
    );
    let data = builder.add_json_field(
        "data",
        JsonObjectOptions::default().set_indexing_options(text_field_indexing),
    );

    SchemaWithMeta {
        schema: builder.build(),
        vertex_addr,
        domain_def_id,
        update_time,
        text,
        data,
    }
}
