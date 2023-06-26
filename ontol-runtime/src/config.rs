#[derive(Default, Debug)]
pub struct PackageConfig {
    pub data_source: Option<DataSourceConfig>,
}

#[derive(Clone, Debug)]
pub enum DataSourceConfig {
    InMemory,
}
