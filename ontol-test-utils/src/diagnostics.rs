use std::collections::BTreeMap;

use ontol_compiler::{
    error::{CompileError, UnifiedCompileError},
    SourceCodeRegistry, SourceId, Sources, SpannedCompileError,
};

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
                        let orig_stripped = if let Some(byte_index) = line.find(space_error_pattern)
                        {
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
            builder.ends_with_newline = matches!(text.as_bytes().last(), Some(b'\n'));

            (*source_id, builder)
        })
        .collect();

    for spanned_error in errors.errors {
        let source_id = spanned_error.span.source_id;
        let builder = builders.get_mut(&source_id).unwrap();

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
        let source_text = source_code_registry.registry.get(source_id).unwrap();

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
