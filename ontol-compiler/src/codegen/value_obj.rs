use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams},
    DefId,
};

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::Ir,
        Block, ProcBuilder, Terminator,
    },
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{equation::TypedExprEquation, ProcTable};

pub(super) fn codegen_value_obj_origin(
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
            builder.push(1, Ir::Clone(self.input_local), *span, block);
        }
    }

    let value_codegen = ValueCodegen {
        input_local: Local(0),
    };
    let mut builder = ProcBuilder::new(NParams(1));
    let mut block = builder.new_block(span);

    CodeGenerator::default().enter_bind_level(value_codegen, |generator| match &to_expr.kind {
        TypedExprKind::ValueObjPattern(expr_ref) => {
            generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *expr_ref);
            block.terminator = Some(Terminator::Return(builder.top()));
        }
        TypedExprKind::MapObjPattern(dest_attrs) => {
            builder.push(
                1,
                Ir::CallBuiltin(BuiltinProc::NewMap, to_def),
                span,
                &mut block,
            );

            for (property_id, node) in dest_attrs {
                generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *node);
                builder.push(
                    -1,
                    Ir::PutAttrValue(Local(1), *property_id),
                    span,
                    &mut block,
                );
            }

            block.terminator = Some(Terminator::Return(builder.top()));
        }
        kind => {
            todo!("target: {kind:?}");
        }
    });

    builder.commit(block);

    builder
}
