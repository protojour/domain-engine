use ontol_runtime::var::Var;
use regex_syntax::hir::{Class, Hir, HirKind, Literal};

use crate::{typed_hir::TypedHir, SourceSpan};

#[derive(Debug)]
pub enum StringInterpolationComponent {
    Const(String),
    Var(Var, SourceSpan),
}

pub struct RegexStringInterpolator<'g, 'c, 'm> {
    pub capture_groups: &'g [ontol_hir::CaptureGroup<'m, TypedHir>],
    pub current_constant: String,
    pub components: &'c mut Vec<StringInterpolationComponent>,
}

impl RegexStringInterpolator<'_, '_, '_> {
    pub fn commit_constant(&mut self) {
        let constant = std::mem::take(&mut self.current_constant);
        if !constant.is_empty() {
            self.components
                .push(StringInterpolationComponent::Const(constant));
        }
    }

    pub fn traverse_hir(&mut self, hir: &Hir) {
        match hir.kind() {
            HirKind::Empty => {}
            HirKind::Literal(Literal(bytes)) => self
                .current_constant
                .push_str(std::str::from_utf8(bytes).unwrap()),
            HirKind::Class(Class::Bytes(bytes)) => {
                let first = bytes.iter().next().unwrap();
                self.current_constant.push(first.start() as char);
            }
            HirKind::Class(Class::Unicode(bytes)) => {
                let first = bytes.iter().next().unwrap();
                self.current_constant.push(first.start());
            }
            HirKind::Look(_) => {}
            HirKind::Repetition(_) => {}
            HirKind::Capture(capture) => {
                if capture.name.is_some() {
                    self.commit_constant();

                    let capture_group = self
                        .capture_groups
                        .iter()
                        .find(|capture_group| capture_group.index == capture.index)
                        .unwrap();

                    self.components.push(StringInterpolationComponent::Var(
                        capture_group.binder.hir().var,
                        capture_group.binder.meta().span,
                    ));
                } else {
                    self.traverse_hir(&capture.sub);
                }
            }
            HirKind::Concat(hirs) => {
                for hir in hirs {
                    self.traverse_hir(hir);
                }
            }
            HirKind::Alternation(alternation) => {
                if let Some(first) = alternation.first() {
                    self.traverse_hir(first);
                }
            }
        }
    }
}
