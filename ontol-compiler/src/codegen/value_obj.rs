use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode},
    DefId,
};
use smallvec::smallvec;
use tracing::debug;

use crate::{
    codegen::{translate::VarFlowTracker, Codegen, SpannedOpCodes},
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind, TypedExprTable},
    SourceSpan,
};

use super::{ProcTable, UnlinkedProc};

pub(super) fn codegen_value_obj_origin(
    proc_table: &mut ProcTable,
    expr_table: &TypedExprTable,
    to: ExprRef,
    to_def: DefId,
) -> UnlinkedProc {
    let (_, to_expr, span) = expr_table.resolve_expr(&expr_table.target_rewrites, to);

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

    match &to_expr.kind {
        TypedExprKind::ValueObjPattern(expr_ref) => {
            value_codegen.codegen_expr(proc_table, expr_table, *expr_ref, &mut opcodes);
            opcodes.push((OpCode::Return0, to_expr.span));
        }
        TypedExprKind::MapObjPattern(dest_attrs) => {
            opcodes.push((OpCode::CallBuiltin(BuiltinProc::NewMap, to_def), span));

            // the input value is not a map, so it will be consumed.
            // Therefore it must be top of the stack:
            opcodes.push((OpCode::Swap(Local(0), Local(1)), span));
            value_codegen.input_local = Local(1);

            for (property_id, node) in dest_attrs {
                value_codegen.codegen_expr(proc_table, expr_table, *node, &mut opcodes);
                opcodes.push((OpCode::PutUnitAttr(Local(0), *property_id), span));
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
                    // Keep cloning until the last use of the variable,
                    // which must pop it off the stack. (i.e. keep the clone instruction).
                    // else: drop clone instruction. Stack should only contain the return value.
                    value_codegen.var_tracker.do_use(SyntaxVar(0)).use_count > 1
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
