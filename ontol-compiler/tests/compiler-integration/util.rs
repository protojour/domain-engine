use std::collections::{BTreeMap, HashMap};

use ontol_compiler::{
    error::{CompileError, UnifiedCompileError},
    SourceCodeRegistry, SourceId, Sources, SpannedCompileError, Src,
};
use ontol_runtime::{
    env::Env,
    serde::SerdeOperatorId,
    value::{Attribute, Data, Value},
    DefId,
};
use serde::de::DeserializeSeed;
use tracing::debug;

use crate::TEST_PKG;

pub struct TypeBinding<'e> {
    pub def_id: DefId,
    serde_operator_id: SerdeOperatorId,
    env: &'e Env,
}

impl<'e> TypeBinding<'e> {
    pub fn new(env: &'e Env, type_name: &str) -> Self {
        let domain = env.get_domain(&TEST_PKG).unwrap();
        let def_id = domain
            .get_def_id(type_name)
            .unwrap_or_else(|| panic!("type name not found: `{type_name}`"));
        let serde_operator_id = domain.get_serde_operator_id(type_name).unwrap();
        let binding = Self {
            def_id,
            serde_operator_id,
            env,
        };
        debug!(
            "deserializing `{type_name}` with {:?}",
            env.new_serde_processor(serde_operator_id)
        );
        binding
    }

    pub fn env(&self) -> &Env {
        self.env
    }

    pub fn deserialize_data(&self, json: serde_json::Value) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_eq!(value.type_def_id, self.def_id);
        Ok(value.data)
    }

    /// Deserialize data, but expect that the resulting type DefId
    /// is not the same as the nominal one for the TypeBinding.
    /// (i.e. it should deserialize to a _variant_ of the type)
    pub fn deserialize_data_variant(
        &self,
        json: serde_json::Value,
    ) -> Result<Data, serde_json::Error> {
        let value = self.deserialize_value(json)?;
        assert_ne!(value.type_def_id, self.def_id);
        Ok(value.data)
    }

    pub fn deserialize_value(&self, json: serde_json::Value) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();
        let Attribute { value, rel_params } = self
            .env
            .new_serde_processor(self.serde_operator_id)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))?;

        assert_eq!(rel_params.type_def_id, DefId::unit());

        Ok(value)
    }

    pub fn serialize_data_json(&self, env: &Env, data: &Data) -> serde_json::Value {
        self.serialize_json(env, &Value::new(data.clone(), self.def_id))
    }

    pub fn serialize_json(&self, env: &Env, value: &Value) -> serde_json::Value {
        let mut buf: Vec<u8> = vec![];
        env.new_serde_processor(self.serde_operator_id)
            .serialize_value(&value, None, &mut serde_json::Serializer::new(&mut buf))
            .expect("serialization failed");
        serde_json::from_slice(&buf).unwrap()
    }
}

pub fn serialize_json(env: &Env, value: &Value) -> serde_json::Value {
    let serde_operator_id = env.serde_operators_per_def.get(&value.type_def_id).unwrap();
    let mut buf: Vec<u8> = vec![];
    env.new_serde_processor(*serde_operator_id)
        .serialize_value(&value, None, &mut serde_json::Serializer::new(&mut buf))
        .expect("serialization failed");
    serde_json::from_slice(&buf).unwrap()
}

#[derive(Default)]
struct DiagnosticBuilder {
    cursor: usize,
    lines: Vec<DiagnosticsLine>,
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
            let builder = text
                .lines()
                .fold(DiagnosticBuilder::default(), |mut builder, line| {
                    let orig_stripped = if let Some(byte_index) = line.find(&space_error_pattern) {
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

    for (source_id, builder) in &builders {
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

                annotated.push_str(" ");
                annotated.push_str(&joined_errors);
            }

            if line_iter.peek().is_some() {
                annotated.push_str("\n");
            }
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
