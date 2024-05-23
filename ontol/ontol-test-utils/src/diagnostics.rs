use std::{collections::BTreeMap, fmt::Display};

use ontol_compiler::{
    error::{CompileError, Note, UnifiedCompileError},
    SourceCodeRegistry, SourceId, SourceSpan, Sources,
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
    messages: Vec<SpannedMessage>,
}

struct SpannedMessage {
    message: Message,
    span: SourceSpan,
}

#[derive(Debug)]
enum Message {
    Error(CompileError),
    Note(Note),
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error(error) => write!(f, "// ERROR {error}"),
            Self::Note(note) => write!(f, "// NOTE {note}"),
        }
    }
}

pub struct AnnotatedCompileError {
    pub compile_error: CompileError,
    pub span_text: String,
}

#[track_caller]
pub fn diff_errors(
    errors: UnifiedCompileError,
    sources: &Sources,
    source_code_registry: &SourceCodeRegistry,
) -> Vec<AnnotatedCompileError> {
    let mut builders: BTreeMap<SourceId, DiagnosticBuilder> = source_code_registry
        .registry
        .iter()
        .map(|(source_id, text)| {
            let mut builder =
                text.lines()
                    .fold(DiagnosticBuilder::default(), |mut builder, line| {
                        let orig_stripped = if let Some(byte_index) = line.find(" // ERROR") {
                            &line[..byte_index]
                        } else {
                            line
                        };
                        let orig_stripped = if let Some(byte_index) = orig_stripped.find(" // NOTE")
                        {
                            &orig_stripped[..byte_index]
                        } else {
                            orig_stripped
                        };

                        builder.lines.push(DiagnosticsLine {
                            start: builder.cursor,
                            orig_stripped: orig_stripped.to_string(),
                            messages: vec![],
                        });
                        builder.cursor += line.len() + 1;
                        builder
                    });

            // .lines() does not record the final newline
            builder.ends_with_newline = matches!(text.as_bytes().last(), Some(b'\n'));

            (*source_id, builder)
        })
        .collect();

    fn add_message_to_builder(
        builders: &mut BTreeMap<SourceId, DiagnosticBuilder>,
        spanned_message: SpannedMessage,
    ) {
        let source_id = spanned_message.span.source_id;
        let builder = match builders.get_mut(&source_id) {
            Some(builder) => builder,
            None => panic!(
                "No builder for {source_id:?}: {:?}",
                spanned_message.message
            ),
        };

        let byte_pos = spanned_message.span.span.start as usize;

        let diagnostics_line = builder
            .lines
            .iter_mut()
            .rev()
            .find(|line| line.start <= byte_pos);

        if let Some(diagnostics_line) = diagnostics_line {
            diagnostics_line.messages.push(spanned_message);
        } else {
            panic!("Did not find originating line in script");
        }
    }

    for spanned_error in errors.errors {
        add_message_to_builder(
            &mut builders,
            SpannedMessage {
                message: Message::Error(spanned_error.error),
                span: spanned_error.span,
            },
        );
        for spanned_note in spanned_error.notes {
            add_message_to_builder(
                &mut builders,
                SpannedMessage {
                    span: spanned_note.span(),
                    message: Message::Note(spanned_note.into_note()),
                },
            );
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

            if !line.messages.is_empty() {
                let joined_errors = line
                    .messages
                    .iter()
                    .map(|spanned_msg| format!("{}", spanned_msg.message))
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
            for spanned_message in line.messages {
                let text = source_code_registry
                    .registry
                    .get(&source_id)
                    .expect("no source text available");

                let span_text = &text[spanned_message.span.span.start as usize
                    ..spanned_message.span.span.end as usize];

                if let Message::Error(compile_error) = spanned_message.message {
                    annotated_errors.push(AnnotatedCompileError {
                        compile_error,
                        span_text: span_text.to_string(),
                    });
                }
            }
        }
    }

    annotated_errors
}
