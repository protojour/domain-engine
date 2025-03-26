use arcstr::ArcStr;
use fnv::FnvHashSet;

#[derive(Default)]
pub struct Cache {
    strings: FnvHashSet<ArcStr>,
}

impl Cache {
    pub fn intern(&mut self, string: &str) -> ArcStr {
        if let Some(arcstr) = self.strings.get(string) {
            return arcstr.clone();
        }

        let arcstr = ArcStr::from(string);
        self.strings.insert(arcstr.clone());
        arcstr
    }

    pub fn gc(&mut self) {
        self.strings
            .retain(|arcstr| ArcStr::strong_count(arcstr) != Some(1));
    }
}
