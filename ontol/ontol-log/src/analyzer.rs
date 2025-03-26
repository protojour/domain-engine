use std::backtrace::Backtrace;

use either::Either;
use ontol_core::property::{PropertyCardinality, ValueCardinality};
use ontol_parser::{
    U32Span,
    cst::{
        inspect::{self as insp},
        view::TypedView,
    },
    lexer::kind::Kind,
};
use ontol_syntax::{rowan::GreenNode, syntax_view::RowanNodeView};
use thin_vec::{ThinVec, thin_vec};
use ulid::Ulid;

use crate::{
    diff::{diff_type_ref_or_union, diff_type_ref_or_union_or_pattern},
    log_model::{
        ArcCoord, ArcProp, DefModifier, DefProp, DomainProp, FmtProp, RelProp,
        TypeRefOrUnionOrPattern, UseProp,
    },
    lookup::{
        LookupTable, lookup_type_path, resolve_quant_type_reference,
        resolve_quant_type_reference_or_pattern, resolve_type_reference,
    },
    symbol::{SymEntry, SymbolTableUpdates},
    syntax_diff::{Change, DiffError, diff, eq_modulo_trivia_view},
    tables::{DomainTablesView, LayeredDomainTables, RelKey},
    tag::{Tag, TagAllocator},
    token::Token,
};

#[derive(Debug)]
pub enum Error {
    Ontol,
    Diff,
    UseSyntax,
    ArcSyntax,
    Syntax(Backtrace),
    SymbolAlreadyDefined,
    PrevRelNotFound,
    Type(Backtrace),
    Todo(String),
}

impl From<DiffError> for Error {
    fn from(_value: DiffError) -> Self {
        Self::Diff
    }
}

#[derive(Clone, Copy)]
pub enum BlockContext {
    NoContext,
    Def(Tag),
    RelParams { def: u16 },
    FmtLeading,
}

type Pair<T> = (T, T);

#[derive(Debug)]
pub enum Event {
    Start(Box<Ulid>),
    DomainAdd(Tag, ThinVec<DomainProp>),
    DomainChange(Tag, ThinVec<DomainProp>),
    DomainRemove(Tag),
    UseAdd(Tag, ThinVec<UseProp>),
    UseChange(Tag, ThinVec<UseProp>),
    UseRemove(Tag),
    DefAdd(Tag, ThinVec<DefProp>),
    DefChange(Tag, ThinVec<DefProp>),
    DefRemove(Tag),
    ArcAdd(Tag, ThinVec<ArcProp>),
    ArcChange(Tag, ThinVec<ArcProp>),
    ArcRemove(Tag),
    RelAdd(Tag, Box<(RelKey, ThinVec<RelProp>)>),
    RelChange(Tag, Box<(RelKey, RelKey, ThinVec<RelProp>)>),
    RelRemove(Tag, Box<RelKey>),
    FmtAdd(Tag, ThinVec<FmtProp>),
    FmtChange(Tag, ThinVec<FmtProp>),
    FmtRemove(Tag),
}

pub struct DomainAnalyzer<'a> {
    layered_tables: LayeredDomainTables<'a>,
    events: Vec<Event>,
    domain_tag: Tag,
    allocator: &'a mut TagAllocator,
    lazy_tasks: Vec<LazyTask>,
    errors: Vec<(Error, U32Span)>,
}

enum LazyTask {
    DefBody(
        Tag,
        Option<insp::DefBody<RowanNodeView>>,
        Option<insp::DefBody<RowanNodeView>>,
    ),
    ArcStmtInner(
        Tag,
        insp::ArcStatement<RowanNodeView>,
        insp::ArcStatement<RowanNodeView>,
    ),
}

impl<'a> DomainAnalyzer<'a> {
    pub fn new(
        domain_tag: Tag,
        tables: DomainTablesView<'a>,
        allocator: &'a mut TagAllocator,
    ) -> Self {
        Self {
            layered_tables: LayeredDomainTables::new(tables),
            events: vec![],
            domain_tag,
            allocator,
            lazy_tasks: vec![],
            errors: Default::default(),
        }
    }

    pub fn finish(self) -> (Vec<Event>, SymbolTableUpdates) {
        (self.events, self.layered_tables.into_updates())
    }

    fn update_tables(&mut self, checkpoint: usize) -> Result<(), Error> {
        for event in &self.events[checkpoint..] {
            match event {
                Event::UseAdd(tag, props) | Event::UseChange(tag, props) => {
                    for prop in props {
                        match prop {
                            UseProp::Ident(ident) => {
                                self.layered_tables
                                    .put_symbol(ident.clone(), || SymEntry::Use(*tag))
                                    .inserted()
                                    .ok_or(Error::SymbolAlreadyDefined)?;
                            }
                            UseProp::Uri(_) => {}
                        }
                    }
                }
                Event::DefAdd(tag, props) | Event::DefChange(tag, props) => {
                    for prop in props {
                        match prop {
                            DefProp::Ident(ident) => {
                                self.layered_tables
                                    .put_symbol(ident.clone(), || SymEntry::LocalDef(*tag))
                                    .inserted()
                                    .ok_or(Error::SymbolAlreadyDefined)?;
                            }
                            DefProp::ChangeIdent(new, old) => {
                                self.layered_tables.remove_symbol(old).type_err()?;
                                self.layered_tables
                                    .put_symbol(new.clone(), || SymEntry::LocalDef(*tag))
                                    .inserted()
                                    .ok_or(Error::SymbolAlreadyDefined)?;
                            }
                            _ => {}
                        }
                    }
                }
                Event::DefRemove(tag) => {
                    self.layered_tables
                        .front_symbols
                        .retain(|_, entry| match entry {
                            Some(SymEntry::LocalDef(def_tag)) => def_tag != tag,
                            _ => true,
                        });
                }
                Event::ArcAdd(tag, props) => {
                    for prop in props {
                        match prop {
                            ArcProp::Domain(domain_tag) => {}
                            ArcProp::Ident(token) => {
                                self.layered_tables
                                    .put_symbol(token.clone(), || SymEntry::LocalEdge(*tag));
                            }
                            ArcProp::SlotSymbol(coord, token) => {
                                self.layered_tables
                                    .put_edge_symbol(*tag, token.clone(), *coord);
                            }
                            ArcProp::Var(arc_coord, green_token) => {}
                            ArcProp::TypeParam(arc_coord, type_ref) => {}
                        }
                    }
                }
                Event::ArcChange(edge_tag, thin_vec) => todo!(),
                Event::ArcRemove(edge_tag) => {}
                _ => {}
            }
        }

        Ok(())
    }

    pub fn ontol(&mut self, new_root: GreenNode, old_root: GreenNode) -> Result<(), Error> {
        let Some(new_ontol) = insp::Ontol::from_view(RowanNodeView::new_root(new_root)) else {
            return Err(Error::Ontol);
        };
        let old_ontol = insp::Ontol::from_view(RowanNodeView::new_root(old_root)).unwrap();

        self.statements(
            new_ontol.statements(),
            old_ontol.statements(),
            BlockContext::NoContext,
        )?;

        while !self.lazy_tasks.is_empty() {
            for task in std::mem::take(&mut self.lazy_tasks) {
                match task {
                    LazyTask::DefBody(tag, cur, prev) => {
                        self.statements(
                            cur.iter().flat_map(|b| b.statements()),
                            prev.iter().flat_map(|b| b.statements()),
                            BlockContext::Def(tag),
                        )?;
                    }
                    LazyTask::ArcStmtInner(tag, cur, prev) => {
                        let props = self.arc_stmt_lazy(cur, prev, tag)?;
                        self.events.push(Event::ArcChange(tag, props));
                    }
                }
            }
        }

        Ok(())
    }

    fn statements(
        &mut self,
        new: impl Iterator<Item = insp::Statement<RowanNodeView>>,
        old: impl Iterator<Item = insp::Statement<RowanNodeView>>,
        context: BlockContext,
    ) -> Result<(), Error> {
        enum StmtChange {
            Domain(Pair<insp::DomainStatement<RowanNodeView>>),
            Use(Pair<insp::UseStatement<RowanNodeView>>),
            Def(Pair<insp::DefStatement<RowanNodeView>>),
            Arc(Pair<insp::ArcStatement<RowanNodeView>>),
            Rel(Pair<insp::RelStatement<RowanNodeView>>),
            Sym(Pair<insp::SymStatement<RowanNodeView>>),
            Fmt(Pair<insp::FmtStatement<RowanNodeView>>),
            Map(Pair<insp::MapStatement<RowanNodeView>>),
        }

        let output = diff(new, old, |new, old| match (new, old) {
            (insp::Statement::DomainStatement(cur), insp::Statement::DomainStatement(prev)) => {
                Some(Change::Interior(StmtChange::Domain((cur, prev))))
            }
            (insp::Statement::UseStatement(cur), insp::Statement::UseStatement(prev)) => {
                if eq_modulo_trivia_view(cur.ident_path()?.view(), prev.ident_path()?.view())
                    || eq_modulo_trivia_view(cur.uri()?.view(), prev.uri()?.view())
                {
                    Some(Change::Interior(StmtChange::Use((cur, prev))))
                } else {
                    Some(Change::Distinct(
                        insp::Statement::UseStatement(cur),
                        insp::Statement::UseStatement(prev),
                    ))
                }
            }
            (insp::Statement::DefStatement(cur), insp::Statement::DefStatement(prev)) => {
                Some(Change::Interior(StmtChange::Def((cur, prev))))
            }
            (insp::Statement::ArcStatement(cur), insp::Statement::ArcStatement(prev)) => {
                Some(Change::Interior(StmtChange::Arc((cur, prev))))
            }
            (insp::Statement::RelStatement(cur), insp::Statement::RelStatement(prev)) => {
                Some(Change::Interior(StmtChange::Rel((cur, prev))))
            }
            (insp::Statement::SymStatement(cur), insp::Statement::SymStatement(prev)) => {
                Some(Change::Interior(StmtChange::Sym((cur, prev))))
            }
            (insp::Statement::FmtStatement(cur), insp::Statement::FmtStatement(prev)) => {
                Some(Change::Interior(StmtChange::Fmt((cur, prev))))
            }
            (insp::Statement::MapStatement(cur), insp::Statement::MapStatement(prev)) => {
                Some(Change::Interior(StmtChange::Map((cur, prev))))
            }
            _ => None,
        })?;

        for change in output.changed {
            let checkpoint = self.events.len();

            match change {
                StmtChange::Domain((cur, prev)) => todo!(),
                StmtChange::Use((cur, prev)) => {
                    let prev_ident = prev.ident_path().stx_err()?.symbols().next().stx_err()?;
                    let tag = self
                        .layered_tables
                        .lookup_root(&prev_ident.0.green().to_owned())
                        .stx_err()?
                        .use_tag()
                        .type_err()?;
                    let props = self.use_stmt(cur, prev)?;
                    if !props.is_empty() {
                        self.events.push(Event::UseChange(tag, props));
                    }
                }
                StmtChange::Def((cur, prev)) => {
                    let prev_ident = prev.ident_path().stx_err()?.symbols().next().stx_err()?;
                    let tag = self
                        .layered_tables
                        .lookup_root(&prev_ident.0.green().to_owned())
                        .stx_err()?
                        .local_def()
                        .type_err()?;
                    let props = self.def_stmt(cur, prev, tag)?;
                    if !props.is_empty() {
                        self.events.push(Event::DefChange(tag, props));
                    }
                }
                StmtChange::Sym((cur, prev)) => {
                    self.sym_stmt(cur, prev)?;
                }
                StmtChange::Arc((cur, prev)) => todo!(),
                StmtChange::Rel((cur, prev)) => {
                    let prev_key =
                        RelKey::new(prev.clone(), context, &self.layered_tables).stx_err()?;
                    let tag = self
                        .layered_tables
                        .back
                        .local
                        .rels
                        .table
                        .get(&prev_key)
                        .ok_or(Error::PrevRelNotFound)?;
                    let props = self.rel_stmt(cur.clone(), prev, *tag, context)?;
                    if !props.is_empty() {
                        let new_key = RelKey::new(cur, context, &self.layered_tables).stx_err()?;
                        self.events
                            .push(Event::RelChange(*tag, Box::new((new_key, prev_key, props))));
                    }
                }
                StmtChange::Fmt((cur, prev)) => todo!(),
                StmtChange::Map((cur, prev)) => todo!(),
            }

            self.update_tables(checkpoint)?;
        }

        for stmt in output.added {
            let checkpoint = self.events.len();

            match stmt {
                insp::Statement::DomainStatement(stmt) => {
                    // self.model.push(kind);
                }
                insp::Statement::UseStatement(stmt) => {
                    let tag = self.allocator.next_tag.bump();
                    let props = self.use_stmt(
                        stmt,
                        TypedView::from_view(RowanNodeView::empty(Kind::UseStatement)).unwrap(),
                    )?;
                    self.events.push(Event::UseAdd(tag, props));
                }
                insp::Statement::DefStatement(stmt) => {
                    let tag = self.allocator.next_tag.bump();
                    let props = self.def_stmt(
                        stmt,
                        TypedView::from_view(RowanNodeView::empty(Kind::DefStatement)).unwrap(),
                        tag,
                    )?;
                    self.events.push(Event::DefAdd(tag, props));
                }
                insp::Statement::SymStatement(stmt) => {
                    self.sym_stmt(
                        stmt,
                        TypedView::from_view(RowanNodeView::empty(Kind::SymStatement)).unwrap(),
                    )?;
                }
                insp::Statement::ArcStatement(stmt) => {
                    let tag = self.allocator.next_tag.bump();
                    let props = self.arc_stmt_outer(
                        stmt,
                        TypedView::from_view(RowanNodeView::empty(Kind::ArcStatement)).unwrap(),
                        tag,
                    )?;
                    self.events.push(Event::ArcAdd(tag, props));
                }
                insp::Statement::RelStatement(stmt) => {
                    let tag = self.allocator.next_tag.bump();
                    let key =
                        RelKey::new(stmt.clone(), context, &self.layered_tables).type_err()?;
                    let props = self.rel_stmt(
                        stmt,
                        TypedView::from_view(RowanNodeView::empty(Kind::RelStatement)).unwrap(),
                        tag,
                        context,
                    )?;
                    self.events.push(Event::RelAdd(tag, Box::new((key, props))));
                }
                insp::Statement::FmtStatement(stmt) => {}
                insp::Statement::MapStatement(stmt) => {}
            }

            self.update_tables(checkpoint)?;
        }

        for prev in output.removed {
            let checkpoint = self.events.len();

            match prev {
                insp::Statement::DomainStatement(prev) => {}
                insp::Statement::UseStatement(prev) => {}
                insp::Statement::DefStatement(prev) => {
                    let prev_ident = prev.ident_path().stx_err()?.symbols().next().stx_err()?;
                    let tag = self
                        .layered_tables
                        .back
                        .lookup_root(&prev_ident.0.green().to_owned())
                        .stx_err()?
                        .local_def()
                        .type_err()?;
                    self.events.push(Event::DefRemove(tag));
                }
                insp::Statement::SymStatement(prev) => {
                    self.sym_stmt(
                        TypedView::from_view(RowanNodeView::empty(Kind::SymStatement)).unwrap(),
                        prev,
                    )?;
                }
                insp::Statement::ArcStatement(prev) => {}
                insp::Statement::RelStatement(prev) => {
                    let prev_key = RelKey::new(prev, context, &self.layered_tables).type_err()?;
                    let tag = self
                        .layered_tables
                        .back
                        .local
                        .rels
                        .table
                        .get(&prev_key)
                        .ok_or(Error::PrevRelNotFound)?;
                    self.events.push(Event::RelRemove(*tag, Box::new(prev_key)));
                }
                insp::Statement::FmtStatement(prev) => {}
                insp::Statement::MapStatement(prev) => {}
            }

            self.update_tables(checkpoint)?;
        }

        Ok(())
    }

    fn use_stmt(
        &mut self,
        cur: insp::UseStatement<RowanNodeView>,
        prev: insp::UseStatement<RowanNodeView>,
    ) -> Result<ThinVec<UseProp>, Error> {
        let mut props = thin_vec![];

        let cur_uri = cur.uri().stx_err()?.text().stx_err()?.ok().stx_err()?;

        if let Some(prev_uri) = prev
            .uri()
            .and_then(|uri| uri.text())
            .and_then(|text| text.ok())
        {
            if cur_uri != prev_uri {
                props.push(UseProp::Uri(cur_uri.into()));
            }
        } else {
            props.push(UseProp::Uri(cur_uri.into()));
        }

        let cur_symbol = cur.ident_path().stx_err()?.symbols().next().stx_err()?;

        if let Some(prev_symbol) = prev.ident_path().and_then(|p| p.symbols().next()) {
            if cur_symbol.0 != prev_symbol.0 {
                props.push(UseProp::Ident(Token::from(cur_symbol)));
            }
        } else {
            props.push(UseProp::Ident(Token::from(cur_symbol)));
        }

        Ok(props)
    }

    fn def_stmt(
        &mut self,
        cur: insp::DefStatement<RowanNodeView>,
        prev: insp::DefStatement<RowanNodeView>,
        tag: Tag,
    ) -> Result<ThinVec<DefProp>, Error> {
        let mut props = thin_vec![];

        let cur_ident = cur.ident_path().stx_err()?.symbols().next().stx_err()?;

        match prev.ident_path().and_then(|p| p.symbols().next()) {
            Some(prev_ident) => {
                if cur_ident.0.text() != prev_ident.0.text() {
                    props.push(DefProp::ChangeIdent(
                        Token::from(cur_ident),
                        Token::from(prev_ident),
                    ));
                }
            }
            None => {
                props.push(DefProp::Ident(Token::from(cur_ident)));
            }
        }

        {
            let output = diff::<_, ()>(cur.modifiers(), prev.modifiers(), |cur, prev| {
                Some(Change::Distinct(cur, prev))
            })?;

            for modifier in output.added {
                props.push(DefProp::ModAdd(def_modifier_prop(modifier).stx_err()?));
            }

            for modifier in output.removed {
                props.push(DefProp::ModRemove(def_modifier_prop(modifier).stx_err()?));
            }
        }

        self.lazy_tasks
            .push(LazyTask::DefBody(tag, cur.body(), prev.body()));

        Ok(props)
    }

    fn sym_stmt(
        &mut self,
        cur: insp::SymStatement<RowanNodeView>,
        prev: insp::SymStatement<RowanNodeView>,
    ) -> Result<(), Error> {
        let output = diff::<_, Pair<insp::SymRelation<RowanNodeView>>>(
            cur.sym_relations(),
            prev.sym_relations(),
            |cur, prev| Some(Change::Interior((cur, prev))),
        )?;

        for (new, old) in output.changed {
            let new_symbol = new.decl().stx_err()?.symbol().stx_err()?;
            let old_symbol = old.decl().stx_err()?.symbol().stx_err()?;

            let tag = self
                .layered_tables
                .back
                .local
                .symbols
                .table
                .get(&old_symbol.0.green().to_owned())
                .type_err()?
                .local_def()
                .type_err()?;

            let mut props = thin_vec![];

            if &new_symbol.0 != &old_symbol.0 {
                props.push(DefProp::ChangeIdent(
                    Token::from(&new_symbol),
                    Token::from(&old_symbol),
                ));
            }

            self.events.push(Event::DefChange(tag, props));
        }

        for sym in output.added {
            let symbol = sym.decl().stx_err()?.symbol().stx_err()?;
            let tag = self.allocator.next_tag.bump();

            self.events.push(Event::DefAdd(
                tag,
                thin_vec![DefProp::Ident(Token::from(symbol)), DefProp::SymSet],
            ));
        }

        Ok(())
    }

    fn arc_stmt_outer(
        &mut self,
        cur: insp::ArcStatement<RowanNodeView>,
        prev: insp::ArcStatement<RowanNodeView>,
        tag: Tag,
    ) -> Result<ThinVec<ArcProp>, Error> {
        let mut need_lazy = false;
        let mut props = thin_vec![];

        let cur_ident = cur.ident_path().stx_err()?.symbols().next().stx_err()?;

        props.push(ArcProp::Ident(Token::from(&cur_ident)));

        for (row, arc_clause) in cur.arc_clauses().enumerate() {
            let row = u8::try_from(row).ok().stx_err()?;

            let mut items = arc_clause.items();

            let insp::ArcItem::ArcVar(var) = items.next().stx_err()? else {
                return Err(Error::ArcSyntax);
            };

            props.push(ArcProp::Var(
                ArcCoord(row, 0),
                Token::from(&var.symbol().stx_err()?),
            ));

            let mut column = 1;

            while let Some(next) = items.next() {
                match next {
                    insp::ArcItem::ArcSlot(slot) => {
                        props.push(ArcProp::SlotSymbol(
                            ArcCoord(row, column),
                            Token::from(&slot.symbol().stx_err()?),
                        ));
                    }
                    insp::ArcItem::ArcVar(_) | insp::ArcItem::ArcTypeParam(_) => {
                        return Err(Error::ArcSyntax);
                    }
                }

                match items.next().stx_err()? {
                    insp::ArcItem::ArcVar(var) => {
                        props.push(ArcProp::Var(
                            ArcCoord(row, column),
                            Token::from(var.symbol().stx_err()?),
                        ));
                    }
                    insp::ArcItem::ArcTypeParam(_type_param) => {
                        // ArcTypeParam is handled in the lazy task,
                        // since its type may be unknown
                        need_lazy = true;
                    }
                    insp::ArcItem::ArcSlot(_) => {
                        return Err(Error::ArcSyntax);
                    }
                }

                column += 1;
            }
        }

        if need_lazy {
            self.lazy_tasks.push(LazyTask::ArcStmtInner(tag, cur, prev));
        }

        Ok(props)
    }

    fn arc_stmt_lazy(
        &mut self,
        cur: insp::ArcStatement<RowanNodeView>,
        prev: insp::ArcStatement<RowanNodeView>,
        tag: Tag,
    ) -> Result<ThinVec<ArcProp>, Error> {
        let mut props = thin_vec![];

        for (row, arc_clause) in cur.arc_clauses().enumerate() {
            let row = u8::try_from(row).ok().stx_err()?;

            let mut items = arc_clause.items();
            items.next().stx_err()?;

            let mut column = 1;

            while items.next().is_some() {
                match items.next().stx_err()? {
                    insp::ArcItem::ArcVar(var) => {}
                    insp::ArcItem::ArcTypeParam(type_param) => {
                        // ArcTypeParam is handled in the lazy task,
                        // since its type may be unknown
                        let type_ref = lookup_type_path(
                            &type_param.ident_path().stx_err()?,
                            &self.layered_tables,
                        )
                        .type_err()?;

                        props.push(ArcProp::TypeParam(ArcCoord(row, column), type_ref));
                    }
                    insp::ArcItem::ArcSlot(_) => {
                        panic!();
                    }
                }

                column += 1;
            }
        }

        Ok(props)
    }

    fn rel_stmt(
        &mut self,
        cur: insp::RelStatement<RowanNodeView>,
        prev: insp::RelStatement<RowanNodeView>,
        tag: Tag,
        context: BlockContext,
    ) -> Result<ThinVec<RelProp>, Error> {
        const SUBJ_PROP_CRD: PropertyCardinality = PropertyCardinality::Mandatory;
        const OBJ_PROP_CRD: PropertyCardinality = PropertyCardinality::Mandatory;

        let mut props = thin_vec![];

        let relation = cur.relation().stx_err()?;

        let cur_rel_type = resolve_type_reference(
            relation.relation_type().stx_err()?.type_ref().stx_err()?,
            &mut ValueCardinality::Unit,
            context,
            &self.layered_tables,
        )
        .type_err()?
        .left()
        .stx_err()?;

        if let Some(prev_rel_type_ref) = prev
            .relation()
            .and_then(|r| r.relation_type())
            .and_then(|r| r.type_ref())
        {
            let prev_rel_type = resolve_type_reference(
                prev_rel_type_ref,
                &mut ValueCardinality::Unit,
                context,
                &self.layered_tables.back,
            )
            .type_err()?
            .left()
            .stx_err()?;

            if cur_rel_type != prev_rel_type {
                props.push(RelProp::Rel(cur_rel_type));
            }
        } else {
            props.push(RelProp::Rel(cur_rel_type));
        }

        let (cur_subj, cur_subj_crd) = resolve_quant_type_reference(
            cur.subject().stx_err()?.type_quant().stx_err()?,
            context,
            &self.layered_tables,
        )
        .type_err()?;

        if let Some(prev_quant) = prev.subject().and_then(|s| s.type_quant()) {
            let (prev_subj, prev_crd) =
                resolve_quant_type_reference(prev_quant, context, &self.layered_tables.back)
                    .type_err()?;

            diff_type_ref_or_union(cur_subj, prev_subj, |add, t| {
                if add {
                    props.push(RelProp::SubjAdd(t.clone()));
                } else {
                    props.push(RelProp::SubjRm(t.clone()));
                }
            });

            if cur_subj_crd != prev_crd {
                props.push(RelProp::SubjCrd((SUBJ_PROP_CRD, cur_subj_crd).into()));
            }
        } else {
            match cur_subj {
                Either::Left(t) => {
                    props.push(RelProp::SubjAdd(t));
                }
                Either::Right(u) => {
                    for t in u {
                        props.push(RelProp::SubjAdd(t));
                    }
                }
            }
            props.push(RelProp::SubjCrd((SUBJ_PROP_CRD, cur_subj_crd).into()));
        }

        let (cur_obj, cur_obj_crd) = resolve_quant_type_reference_or_pattern(
            cur.object().stx_err()?.type_quant_or_pattern().stx_err()?,
            context,
            &self.layered_tables,
        )
        .type_err()?;

        if let Some(prev_qp) = prev.object().and_then(|o| o.type_quant_or_pattern()) {
            let (prev_obj, prev_crd) = resolve_quant_type_reference_or_pattern(
                prev_qp,
                context,
                &self.layered_tables.back,
            )
            .type_err()?;

            diff_type_ref_or_union_or_pattern(cur_obj, prev_obj, |add, t| match (add, t) {
                (true, Either::Left(ty)) => {
                    props.push(RelProp::ObjAdd(ty.clone()));
                }
                (true, Either::Right(pat)) => {
                    props.push(RelProp::ObjPatternSet(pat));
                }
                (false, Either::Left(ty)) => {
                    props.push(RelProp::ObjRm(ty.clone()));
                }
                (false, Either::Right(_pat)) => {
                    props.push(RelProp::ObjPatternRm);
                }
            });

            if cur_obj_crd != prev_crd {
                props.push(RelProp::ObjCrd((OBJ_PROP_CRD, cur_subj_crd).into()));
            }
        } else {
            match cur_obj {
                TypeRefOrUnionOrPattern::Type(t) => {
                    props.push(RelProp::ObjAdd(t));
                }
                TypeRefOrUnionOrPattern::Union(u) => {
                    for t in u {
                        props.push(RelProp::ObjAdd(t));
                    }
                }
                TypeRefOrUnionOrPattern::Pattern(pattern) => {
                    props.push(RelProp::ObjPatternSet(pattern));
                }
            }

            props.push(RelProp::ObjCrd((OBJ_PROP_CRD, cur_obj_crd).into()));
        }

        Ok(props)
    }
}

fn def_modifier_prop(modifier: insp::Modifier<RowanNodeView>) -> Option<DefModifier> {
    match modifier.token()?.0.text() {
        "@private" => Some(DefModifier::Private),
        "@open" => Some(DefModifier::Open),
        "@extern" => Some(DefModifier::Extern),
        "@macro" => Some(DefModifier::Macro),
        "@crdt" => Some(DefModifier::Crdt),
        _ => None,
    }
}

trait OptionExt<T> {
    /// Turn into a syntax error
    fn stx_err(self) -> Result<T, Error>;

    /// Turn into a type error
    fn type_err(self) -> Result<T, Error>;
}

impl<T> OptionExt<T> for Option<T> {
    fn stx_err(self) -> Result<T, Error> {
        self.ok_or(Error::Syntax(Backtrace::capture()))
    }

    fn type_err(self) -> Result<T, Error> {
        self.ok_or(Error::Type(Backtrace::capture()))
    }
}
