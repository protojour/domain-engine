use std::{cell::RefCell, rc::Rc};

use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams},
    DefId,
};
use tracing::debug;

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::Ir,
        translate::VarFlowTracker,
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
        var_tracker: VarFlowTracker,
    }

    impl CodegenVariable for Rc<RefCell<ValueCodegen>> {
        fn codegen_variable(
            &mut self,
            builder: &mut ProcBuilder,
            block: &mut Block,
            var: SyntaxVar,
            span: &SourceSpan,
        ) {
            let mut this = self.borrow_mut();
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            this.var_tracker.count_use(var);
            builder.push(1, Ir::Clone(this.input_local), *span, block);
        }
    }

    let value_codegen = Rc::new(RefCell::new(ValueCodegen {
        input_local: Local(0),
        var_tracker: Default::default(),
    }));
    let mut builder = ProcBuilder::new(NParams(1));

    let block =
        CodeGenerator::default().enter_bind_level(
            value_codegen.clone(),
            |generator| match &to_expr.kind {
                TypedExprKind::ValueObjPattern(expr_ref) => {
                    let mut block = builder.new_block(Terminator::Return(Local(0)), span);
                    generator.codegen_expr(
                        proc_table,
                        &mut builder,
                        &mut block,
                        equation,
                        *expr_ref,
                    );
                    block
                }
                TypedExprKind::MapObjPattern(dest_attrs) => {
                    let mut block = builder.new_block(Terminator::Return(Local(1)), span);
                    builder.push(
                        1,
                        Ir::CallBuiltin(BuiltinProc::NewMap, to_def),
                        span,
                        &mut block,
                    );

                    for (property_id, node) in dest_attrs {
                        generator.codegen_expr(
                            proc_table,
                            &mut builder,
                            &mut block,
                            equation,
                            *node,
                        );
                        builder.push(
                            -1,
                            Ir::PutAttrValue(Local(1), *property_id),
                            span,
                            &mut block,
                        );
                    }

                    block
                }
                kind => {
                    todo!("target: {kind:?}");
                }
            },
        );

    /*
    block.opcodes = block
        .opcodes
        .into_iter()
        .filter(|op| {
            match op {
                (OpCode::Clone(local), _) if *local == value_codegen_mut.input_local => {
                    // Keep cloning until the last use of the variable,
                    // which must pop it off the stack. (i.e. keep the clone instruction).
                    // else: drop clone instruction. Stack should only contain the return value.
                    value_codegen_mut
                        .var_tracker
                        .do_use(SyntaxVar(0, BindDepth(0)))
                        .use_count
                        > 1
                }
                _ => true,
            }
        })
        .collect::<SpannedOpCodes>();
    */

    debug!("{:#?}", block.ir);

    builder.commit(block);

    builder
}
