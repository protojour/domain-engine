use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode},
    DefId,
};
use smallvec::smallvec;
use tracing::debug;

use crate::{
    codegen::{
        codegen::{SpannedOpCodes, VarFlowTracker},
        Codegen,
    },
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind, TypedExprTable},
    SourceSpan,
};

use super::{ProcTable, UnlinkedProc};

pub(super) fn codegen_value_obj_origin<'m>(
    proc_table: &mut ProcTable,
    return_def_id: DefId,
    expr_table: &TypedExprTable<'m>,
    to: ExprRef,
) -> UnlinkedProc {
    let (_, dest_expr, span) = expr_table.resolve_expr(&expr_table.target_rewrites, to);

    struct ValueCodegen {
        input_local: Local,
        var_tracker: VarFlowTracker,
    }

    impl Codegen for ValueCodegen {
        fn codegen_variable(
            &mut self,
            var: SyntaxVar,
            opcodes: &mut SpannedOpCodes,
            span: &SourceSpan,
        ) {
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            self.var_tracker.count_use(var);
            opcodes.push((OpCode::Clone(self.input_local), *span));
        }
    }

    let mut value_codegen = ValueCodegen {
        input_local: Local(0),
        var_tracker: Default::default(),
    };
    let mut opcodes = smallvec![];

    match &dest_expr.kind {
        TypedExprKind::ValueObjPattern(expr_ref) => {
            value_codegen.codegen_expr(proc_table, expr_table, *expr_ref, &mut opcodes);
            opcodes.push((OpCode::Return0, dest_expr.span));
        }
        TypedExprKind::MapObjPattern(dest_attrs) => {
            opcodes.push((
                OpCode::CallBuiltin(BuiltinProc::NewMap, return_def_id),
                span,
            ));

            // the input value is not compound, so it will be consumed.
            // Therefore it must be top of the stack:
            opcodes.push((OpCode::Swap(Local(0), Local(1)), span));
            value_codegen.input_local = Local(1);

            for (relation_id, node) in dest_attrs {
                value_codegen.codegen_expr(proc_table, expr_table, *node, &mut opcodes);
                opcodes.push((OpCode::PutAttr(Local(0), *relation_id), span));
            }

            opcodes.push((OpCode::Return0, span));
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }

    let opcodes = opcodes
        .into_iter()
        .filter(|op| {
            match op {
                (OpCode::Clone(local), _) if *local == value_codegen.input_local => {
                    if value_codegen.var_tracker.do_use(SyntaxVar(0)).use_count > 1 {
                        // Keep cloning until the last use of the variable,
                        // which must pop it off the stack. (i.e. keep the clone instruction)
                        true
                    } else {
                        // drop clone instruction. Stack should only contain the return value.
                        false
                    }
                }
                _ => true,
            }
        })
        .collect::<SpannedOpCodes>();

    debug!("{opcodes:#?}");

    UnlinkedProc {
        n_params: NParams(1),
        opcodes,
    }
}
