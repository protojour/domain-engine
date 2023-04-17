use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams},
    DefId,
};

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::Ir,
        proc_builder::Stack,
        Block, ProcBuilder, Terminator,
    },
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{equation::TypedExprEquation, ProcTable};

pub(super) fn codegen_value_pattern_origin(
    proc_table: &mut ProcTable,
    equation: &TypedExprEquation,
    to: ExprRef,
    to_def: DefId,
) -> ProcBuilder {
    let (_, to_expr, span) = equation.resolve_expr(&equation.expansions, to);

    struct ValueCodegen {
        input_local: Local,
    }

    impl CodegenVariable for ValueCodegen {
        fn codegen_variable(
            &mut self,
            builder: &mut ProcBuilder,
            block: &mut Block,
            var: SyntaxVar,
            span: &SourceSpan,
        ) {
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            builder.gen(block, Ir::Clone(self.input_local), Stack(1), *span);
        }
    }

    let value_codegen = ValueCodegen {
        input_local: Local(0),
    };
    let mut builder = ProcBuilder::new(NParams(1));
    let mut block = builder.new_block(Stack(1), span);

    let terminator =
        CodeGenerator::default().enter_bind_level(value_codegen, |generator| match &to_expr.kind {
            TypedExprKind::ValuePattern(expr_ref) => {
                generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *expr_ref);
                Terminator::Return(builder.top())
            }
            TypedExprKind::StructPattern(dest_attrs) => {
                builder.gen(
                    &mut block,
                    Ir::CallBuiltin(BuiltinProc::NewMap, to_def),
                    Stack(1),
                    span,
                );

                for (property_id, node) in dest_attrs {
                    generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *node);
                    builder.gen(
                        &mut block,
                        Ir::PutAttrValue(Local(1), *property_id),
                        Stack(-1),
                        span,
                    );
                }

                Terminator::Return(builder.top())
            }
            kind => {
                todo!("target: {kind:?}");
            }
        });

    builder.commit(block, terminator);

    builder
}
