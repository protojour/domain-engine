use crate::{analyzer::Event, log_model::EKind, tables::DomainTables};

pub fn apply_events(events: Vec<Event>, tables: &mut DomainTables) -> Vec<EKind> {
    let mut log = Vec::with_capacity(events.len());

    for event in events {
        match event {
            Event::Start(ulid) => {
                log.push(EKind::Start(ulid));
            }
            Event::DomainAdd(tag, props) => {
                log.push(EKind::DomainAdd(tag, props));
            }
            Event::DomainChange(tag, props) => {
                log.push(EKind::DomainChange(tag, props));
            }
            Event::DomainRemove(tag) => {
                log.push(EKind::DomainRemove(tag));
            }
            Event::UseAdd(tag, props) => {
                log.push(EKind::UseAdd(tag, props));
            }
            Event::UseChange(tag, props) => {
                log.push(EKind::UseChange(tag, props));
            }
            Event::UseRemove(tag) => {
                log.push(EKind::UseRemove(tag));
            }
            Event::DefAdd(tag, props) => {
                log.push(EKind::DefAdd(tag, props));
            }
            Event::DefChange(tag, props) => {
                log.push(EKind::DefChange(tag, props));
            }
            Event::DefRemove(tag) => {
                log.push(EKind::DefRemove(tag));
            }
            Event::ArcAdd(tag, props) => {
                log.push(EKind::ArcAdd(tag, props));
            }
            Event::ArcChange(tag, props) => {
                log.push(EKind::ArcChange(tag, props));
            }
            Event::ArcRemove(_tag) => {}
            Event::RelAdd(tag, arg) => {
                let (key, props) = *arg;
                tables.rels.table.insert(key, tag);
                log.push(EKind::RelAdd(tag, props));
            }
            Event::RelChange(tag, arg) => {
                let (new_key, old_key, props) = *arg;
                tables.rels.table.remove(&old_key).unwrap();
                tables.rels.table.insert(new_key, tag);
                log.push(EKind::RelChange(tag, props));
            }
            Event::RelRemove(tag, key) => {
                tables.rels.table.remove(&key).unwrap();
                log.push(EKind::RelRemove(tag));
            }
            Event::FmtAdd(tag, props) => {
                log.push(EKind::FmtAdd(tag, props));
            }
            Event::FmtChange(tag, props) => {
                log.push(EKind::FmtChange(tag, props));
            }
            Event::FmtRemove(tag) => {
                log.push(EKind::FmtRemove(tag));
            }
        }
    }

    log
}
