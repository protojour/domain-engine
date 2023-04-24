use ontol_runtime::{
    proc::{BuiltinProc, Local},
    DefId,
};

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::Ir,
        proc_builder::Stack,
        Block, ProcBuilder,
    },
    hir_node::{HirIdx, HirKind, HirVariable},
    SourceSpan,
};

use super::equation::HirEquation;

pub(super) fn codegen_value_pattern_origin(
    gen: &mut CodeGenerator,
    block: &mut Block,
    equation: &HirEquation,
    to: HirIdx,
    to_def: DefId,
) {
    let (_, to_expr, span) = equation.resolve_node(&equation.expansions, to);

    struct ValueCodegen {
        input_local: Local,
    }

    impl CodegenVariable for ValueCodegen {
        fn codegen_variable(
            &mut self,
            builder: &mut ProcBuilder,
            block: &mut Block,
            var: HirVariable,
            span: &SourceSpan,
        ) {
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            builder.push(block, Ir::Clone(self.input_local), Stack(1), *span);
        }
    }

    let value_codegen = ValueCodegen {
        input_local: Local(0),
    };

    gen.enter_bind_level(value_codegen, |generator| match &to_expr.kind {
        HirKind::ValuePattern(node_id) => {
            generator.codegen_expr(block, equation, *node_id);
        }
        HirKind::StructPattern(dest_attrs) => {
            generator.builder.push(
                block,
                Ir::CallBuiltin(BuiltinProc::NewMap, to_def),
                Stack(1),
                span,
            );

            for (property_id, node) in dest_attrs {
                generator.codegen_expr(block, equation, *node);
                generator.builder.push(
                    block,
                    Ir::PutAttrValue(Local(1), *property_id),
                    Stack(-1),
                    span,
                );
            }
        }
        kind => {
            todo!("target: {kind:?}");
        }
    })
}
