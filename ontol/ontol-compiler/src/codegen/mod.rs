pub mod task;
pub mod type_mapper;

mod auto_map;
mod code_generator;
mod data_flow_analyzer;
mod ir;
mod link;
mod optimize;
mod proc_builder;
mod static_condition;
mod union_map_generator;

#[cfg(test)]
mod tests;
