use std::collections::{BTreeMap, HashMap};

use jsonschema::JSONSchema;
use ontol_compiler::{
    error::{CompileError, UnifiedCompileError},
    SourceCodeRegistry, SourceId, Sources, SpannedCompileError, Src,
};
use ontol_runtime::{
    env::{Env, TypeInfo},
    json_schema::build_standalone_schema,
    serde::SerdeOperatorId,
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};
use serde::de::DeserializeSeed;
use tracing::debug;

use crate::TEST_PKG;

pub struct TypeBinding<'e> {
    pub type_info: TypeInfo,
    json_schema: JSONSchema,
    env: &'e Env,
}

impl<'e> TypeBinding<'e> {
    pub fn new(env: &'e Env, type_name: &str) -> Self {
        let domain = env.get_domain(&TEST_PKG).unwrap();
        let type_info = domain
            .types
            .get(type_name)
            .unwrap_or_else(|| panic!("type name not found: `{type_name}`"))
            .clone();

        debug!(
            "TypeBinding::new `{type_name}` with {operator_id:?} {processor:?}",
            operator_id = type_info.serde_operator_id,
            processor = type_info
                .serde_operator_id
                .map(|id| env.new_serde_processor(id, None))
        );

        let json_schema = compile_json_schema(env, &type_info);

        let binding = Self {
            type_info,
            json_schema,
            env,
        };

        binding
    }

    pub fn env(&self) -> &Env {
        self.env
    }

    fn serde_operator_id(&self) -> SerdeOperatorId {
        self.type_info
            .serde_operator_id
            .expect("No serde operator id")
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.env
            .new_serde_processor(self.serde_operator_id(), None)
            .find_property(prop)
    }

    pub fn deserialize_data(&self, json: serde_json::Value) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_eq!(value.type_def_id, self.type_info.def_id);
        Ok(value.data)
    }

    pub fn deserialize_data_map(
        &self,
        json: serde_json::Value,
    ) -> Result<BTreeMap<PropertyId, Attribute>, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_eq!(value.type_def_id, self.type_info.def_id);
        match value.data {
            Data::Map(map) => Ok(map),
            other => panic!("not a map: {other:?}"),
        }
    }

    /// Deserialize data, but expect that the resulting type DefId
    /// is not the same as the nominal one for the TypeBinding.
    /// (i.e. it should deserialize to a _variant_ of the type)
    pub fn deserialize_data_variant(
        &self,
        json: serde_json::Value,
    ) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_ne!(value.type_def_id, self.type_info.def_id);
        Ok(value.data)
    }

    pub fn deserialize_value(&self, json: serde_json::Value) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();

        let attribute_result = self
            .env
            .new_serde_processor(self.serde_operator_id(), None)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string));

        let json_schema_result = self.json_schema.validate(&json);

        match (attribute_result, json_schema_result) {
            (Ok(Attribute { value, rel_params }), Ok(())) => {
                assert_eq!(rel_params.type_def_id, DefId::unit());

                Ok(value)
            }
            (Err(json_error), Err(_)) => Err(json_error),
            (Ok(attribute), Err(validation_errors)) => {
                for error in validation_errors {
                    println!("JSON schema error: {error:?}");
                }
                panic!("BUG: JSON schema did not accept input {json_string}");
            }
            (Err(json_error), Ok(())) => {
                panic!(
                    "BUG: Deserializer did not accept input, but JSONSchema did: {json_error:?}. input={json_string}"
                );
            }
        }
    }

    pub fn serialize_data_json(&self, data: &Data) -> serde_json::Value {
        self.serialize_json(&Value::new(data.clone(), self.type_info.def_id))
    }

    pub fn serialize_json(&self, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        self.env
            .new_serde_processor(self.serde_operator_id(), None)
            .serialize_value(&value, None, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}

fn compile_json_schema(env: &Env, type_info: &TypeInfo) -> JSONSchema {
    let standalone_schema = build_standalone_schema(env, &type_info).unwrap();

    debug!(
        "outputted json schema: {}",
        serde_json::to_string_pretty(&standalone_schema).unwrap()
    );

    JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .compile(&serde_json::to_value(&standalone_schema).unwrap())
        .unwrap()
}

#[derive(Default)]
struct DiagnosticBuilder {
    cursor: usize,
    lines: Vec<DiagnosticsLine>,
    ends_with_newline: bool,
}

struct DiagnosticsLine {
    start: usize,
    orig_stripped: String,
    errors: Vec<SpannedCompileError>,
}

pub struct AnnotatedCompileError {
    pub compile_error: CompileError,
    pub span_text: String,
}

pub fn diff_errors(
    errors: UnifiedCompileError,
    sources: &Sources,
    source_code_registry: &SourceCodeRegistry,
) -> Vec<AnnotatedCompileError> {
    let error_pattern = "// ERROR";
    let space_error_pattern = " // ERROR";

    let mut builders: BTreeMap<SourceId, DiagnosticBuilder> = source_code_registry
        .registry
        .iter()
        .map(|(source_id, text)| {
            let mut builder =
                text.lines()
                    .fold(DiagnosticBuilder::default(), |mut builder, line| {
                        let orig_stripped =
                            if let Some(byte_index) = line.find(&space_error_pattern) {
                                &line[..byte_index]
                            } else {
                                line
                            };

                        builder.lines.push(DiagnosticsLine {
                            start: builder.cursor,
                            orig_stripped: orig_stripped.to_string(),
                            errors: vec![],
                        });
                        builder.cursor += line.len() + 1;
                        builder
                    });

            // .lines() does not record the final newline
            match text.chars().last() {
                Some('\n') => {
                    builder.ends_with_newline = true;
                }
                _ => {}
            }

            (*source_id, builder)
        })
        .collect();

    for spanned_error in errors.errors {
        let source_id = spanned_error.span.source_id;
        let mut builder = builders.get_mut(&source_id).unwrap();

        let byte_pos = spanned_error.span.start as usize;

        let diagnostics_line = builder
            .lines
            .iter_mut()
            .rev()
            .find(|line| line.start <= byte_pos);

        if let Some(diagnostics_line) = diagnostics_line {
            diagnostics_line.errors.push(spanned_error);
        } else {
            panic!("Did not find originating line in script");
        }
    }

    let mut original = String::new();
    let mut annotated = String::new();

    for (source_id, builder) in builders.iter().rev() {
        let source = sources.get_source(*source_id).unwrap();
        let source_text = source_code_registry.registry.get(&source_id).unwrap();

        let source_header = format!("\n// source '{}':\n", source.name);

        original.push_str(&source_header);
        original += source_text;

        annotated.push_str(&source_header);

        let mut line_iter = builder.lines.iter().peekable();
        while let Some(line) = line_iter.next() {
            let orig_stripped = &line.orig_stripped;
            annotated.push_str(orig_stripped);

            if !line.errors.is_empty() {
                let joined_errors = line
                    .errors
                    .iter()
                    .map(|spanned_error| format!("{} {}", error_pattern, spanned_error.error))
                    .collect::<Vec<_>>()
                    .join("");

                annotated.push(' ');
                annotated.push_str(&joined_errors);
            }

            if line_iter.peek().is_some() {
                annotated.push('\n');
            }
        }

        if builder.ends_with_newline {
            annotated.push('\n');
        }
    }

    pretty_assertions::assert_eq!(original, annotated);

    let mut annotated_errors = vec![];

    for (source_id, builder) in builders {
        for line in builder.lines {
            for spanned_error in line.errors {
                let text = source_code_registry
                    .registry
                    .get(&source_id)
                    .expect("no source text available");

                let span_text =
                    &text[spanned_error.span.start as usize..spanned_error.span.end as usize];

                annotated_errors.push(AnnotatedCompileError {
                    compile_error: spanned_error.error,
                    span_text: span_text.to_string(),
                })
            }
        }
    }

    annotated_errors
}
