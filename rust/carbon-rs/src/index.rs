//! Carbonserver's in-memory metric catalog and Graphite glob matching.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::RwLock;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexMode {
    Trie,
    Trigram,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricMeta {
    pub logical_size: u64,
    pub physical_size: u64,
    pub data_points: u64,
    pub first_seen_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Match {
    pub path: String,
    pub is_leaf: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GlobError(pub String);

impl fmt::Display for GlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for GlobError {}

/// A Graphite glob. `*` and `?` do not cross metric path separators.
#[derive(Clone, Debug)]
pub struct Glob {
    pattern: String,
}

impl Glob {
    pub fn new(pattern: impl Into<String>) -> Result<Self, GlobError> {
        let pattern = pattern.into();
        if pattern.len() > 4096 {
            return Err(GlobError("glob: pattern exceeds 4096 bytes".into()));
        }
        if pattern
            .bytes()
            .filter(|b| matches!(*b, b'*' | b'?' | b'{' | b'['))
            .count()
            > 128
        {
            return Err(GlobError("glob: pattern has too many operators".into()));
        }
        validate(&pattern)?;
        Ok(Self { pattern })
    }

    pub fn matches(&self, path: &str) -> bool {
        self.matches_checked(path, 100_000).unwrap_or(false)
    }
    pub fn matches_checked(&self, path: &str, work_limit: usize) -> Result<bool, GlobError> {
        matches_at(self.pattern.as_bytes(), path.as_bytes(), work_limit)
    }
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    fn trigrams(&self) -> Vec<[u8; 3]> {
        let mut result = BTreeSet::new();
        let bytes = self.pattern.as_bytes();
        let mut start = 0;
        let mut i = 0;
        while i < bytes.len() {
            if matches!(bytes[i], b'*' | b'?' | b'[' | b'{') {
                add_trigrams(&bytes[start..i], &mut result);
                if bytes[i] == b'[' {
                    while i < bytes.len() && bytes[i] != b']' {
                        i += 1;
                    }
                }
                if bytes[i] == b'{' {
                    let mut depth = 1;
                    while depth > 0 {
                        i += 1;
                        match bytes[i] {
                            b'{' => depth += 1,
                            b'}' => depth -= 1,
                            _ => {}
                        }
                    }
                }
                start = i + 1;
            }
            i += 1;
        }
        add_trigrams(&bytes[start..], &mut result);
        result.into_iter().collect()
    }

    fn literal_prefix(&self) -> &str {
        let end = self
            .pattern
            .find(['*', '?', '[', '{'])
            .unwrap_or(self.pattern.len());
        let before = &self.pattern[..end];
        if end == self.pattern.len() {
            return before;
        }
        let trimmed = before.trim_end_matches('.');
        if before.ends_with('.') {
            trimmed
        } else {
            trimmed.rsplit_once('.').map_or("", |(prefix, _)| prefix)
        }
    }
}

fn add_trigrams(literal: &[u8], out: &mut BTreeSet<[u8; 3]>) {
    for triple in literal.windows(3) {
        out.insert([triple[0], triple[1], triple[2]]);
    }
}

fn validate(pattern: &str) -> Result<(), GlobError> {
    let bytes = pattern.as_bytes();
    let mut braces = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => {
                let end = bytes[i + 1..]
                    .iter()
                    .position(|&b| b == b']')
                    .map(|n| i + n + 1)
                    .ok_or_else(|| GlobError("glob: missing ]".into()))?;
                let mut j = i + 1;
                if j < end && bytes[j] == b'^' {
                    j += 1;
                }
                if j == end {
                    return Err(GlobError("glob: empty range".into()));
                }
                while j < end {
                    if bytes[j] == b'-' {
                        if j == i + 1 || j + 1 == end {
                            return Err(GlobError("glob: invalid range".into()));
                        }
                        if bytes[j - 1] > bytes[j + 1] {
                            return Err(GlobError(
                                "glob: range start is bigger than range end".into(),
                            ));
                        }
                    }
                    j += 1;
                }
                i = end;
            }
            b'{' => braces += 1,
            b'}' => {
                if braces == 0 {
                    return Err(GlobError("glob: missing {".into()));
                }
                braces -= 1;
            }
            _ => {}
        }
        i += 1;
    }
    if braces != 0 {
        return Err(GlobError("glob: missing }".into()));
    }
    Ok(())
}

fn matches_at(pattern: &[u8], path: &[u8], mut work: usize) -> Result<bool, GlobError> {
    fn inner(p: &[u8], s: &[u8], work: &mut usize) -> Result<bool, GlobError> {
        if *work == 0 {
            return Err(GlobError("glob: work limit exceeded".into()));
        }
        *work -= 1;
        if p.is_empty() {
            return Ok(s.is_empty());
        }
        match p[0] {
            b'*' => {
                let mut n = 0;
                while n <= s.len() && (n == 0 || s[n - 1] != b'.') {
                    if inner(&p[1..], &s[n..], work)? {
                        return Ok(true);
                    }
                    n += 1;
                }
                Ok(false)
            }
            b'?' => Ok(!s.is_empty() && s[0] != b'.' && inner(&p[1..], &s[1..], work)?),
            b'[' => {
                let end = p.iter().position(|&b| b == b']').expect("validated glob");
                Ok(!s.is_empty()
                    && s[0] != b'.'
                    && class_matches(&p[1..end], s[0])
                    && inner(&p[end + 1..], &s[1..], work)?)
            }
            b'{' => {
                let end = matching_brace(p).expect("validated glob");
                let tail = &p[end + 1..];
                for alt in split_alternatives(&p[1..end]) {
                    let mut combined = alt;
                    combined.extend_from_slice(tail);
                    if inner(&combined, s, work)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            c => Ok(!s.is_empty() && c == s[0] && inner(&p[1..], &s[1..], work)?),
        }
    }
    inner(pattern, path, &mut work)
}

fn class_matches(class: &[u8], byte: u8) -> bool {
    let (negated, class) = if class.first() == Some(&b'^') {
        (true, &class[1..])
    } else {
        (false, class)
    };
    let mut found = false;
    let mut i = 0;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == b'-' {
            found |= class[i] <= byte && byte <= class[i + 2];
            i += 3;
        } else {
            found |= class[i] == byte;
            i += 1;
        }
    }
    found != negated
}

fn matching_brace(pattern: &[u8]) -> Option<usize> {
    let mut depth = 0;
    for (i, &b) in pattern.iter().enumerate() {
        if b == b'{' {
            depth += 1;
        }
        if b == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
    }
    None
}

fn split_alternatives(input: &[u8]) -> Vec<Vec<u8>> {
    let mut out = vec![];
    let (mut depth, mut start) = (0, 0);
    for (i, &b) in input.iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => depth -= 1,
            b',' if depth == 0 => {
                out.push(input[start..i].to_vec());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(input[start..].to_vec());
    out
}

#[derive(Default)]
struct TrieNode {
    leaf: bool,
    children: BTreeMap<String, TrieNode>,
}

impl TrieNode {
    fn insert(&mut self, metric: &str) {
        let mut node = self;
        for segment in metric.split('.') {
            node = node.children.entry(segment.to_owned()).or_default();
        }
        node.leaf = true;
    }
    fn remove(&mut self, segments: &[&str]) -> bool {
        if segments.is_empty() {
            self.leaf = false;
        } else if let Some(child) = self.children.get_mut(segments[0])
            && child.remove(&segments[1..])
        {
            self.children.remove(segments[0]);
        }
        !self.leaf && self.children.is_empty()
    }
    fn find_matches(
        &self,
        prefix: &str,
        glob: &Glob,
        out: &mut Vec<Match>,
        limit: usize,
        visited: &mut usize,
    ) -> Result<(), GlobError> {
        if out.len() >= limit {
            return Ok(());
        }
        *visited += 1;
        if *visited > 10_000_000 {
            return Err(GlobError("index query exceeds traversal budget".into()));
        }
        if !prefix.is_empty() && glob.matches_checked(prefix, 100_000)? {
            out.push(Match {
                path: prefix.to_owned(),
                is_leaf: self.leaf,
            });
        }
        for (name, child) in &self.children {
            if out.len() >= limit {
                break;
            }
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}.{name}")
            };
            child.find_matches(&path, glob, out, limit, visited)?;
        }
        Ok(())
    }
    fn node<'a>(&'a self, path: &str) -> Option<&'a TrieNode> {
        let mut node = self;
        for segment in path.split('.').filter(|s| !s.is_empty()) {
            node = node.children.get(segment)?;
        }
        Some(node)
    }
}

#[derive(Default)]
struct State {
    metrics: BTreeMap<String, MetricMeta>,
    trie: TrieNode,
    trigrams: HashMap<[u8; 3], BTreeSet<String>>,
    generation: u64,
    revisions: HashMap<String, u64>,
}

pub struct Index {
    mode: IndexMode,
    state: RwLock<State>,
}

impl Index {
    pub fn new(mode: IndexMode) -> Self {
        Self {
            mode,
            state: RwLock::new(State::default()),
        }
    }
    pub fn mode(&self) -> IndexMode {
        self.mode
    }
    pub fn generation(&self) -> u64 {
        self.state.read().expect("index lock poisoned").generation
    }
    pub fn snapshot(&self) -> Vec<(String, MetricMeta, u64)> {
        let state = self.state.read().expect("index lock poisoned");
        state
            .metrics
            .iter()
            .map(|(name, meta)| {
                (
                    name.clone(),
                    meta.clone(),
                    state.revisions.get(name).copied().unwrap_or(0),
                )
            })
            .collect()
    }
    pub fn get(&self, metric: &str) -> Option<MetricMeta> {
        self.state
            .read()
            .expect("index lock poisoned")
            .metrics
            .get(metric)
            .cloned()
    }
    pub fn list(&self) -> Vec<String> {
        self.state
            .read()
            .expect("index lock poisoned")
            .metrics
            .keys()
            .cloned()
            .collect()
    }

    pub fn upsert(&self, metric: &str, meta: MetricMeta) {
        if metric.is_empty() {
            return;
        }
        let mut state = self.state.write().expect("index lock poisoned");
        if !state.metrics.contains_key(metric) {
            state.trie.insert(metric);
            for path in prefixes(metric)
                .into_iter()
                .filter(|_| self.mode == IndexMode::Trigram)
            {
                for tri in path.as_bytes().windows(3) {
                    state
                        .trigrams
                        .entry([tri[0], tri[1], tri[2]])
                        .or_default()
                        .insert(path.clone());
                }
            }
        }
        state.metrics.insert(metric.to_owned(), meta);
        state.generation = state.generation.wrapping_add(1);
        let revision = state.generation;
        state.revisions.insert(metric.to_owned(), revision);
    }

    pub fn remove(&self, metric: &str) -> bool {
        let mut state = self.state.write().expect("index lock poisoned");
        if state.metrics.remove(metric).is_none() {
            return false;
        }
        state.trie.remove(&metric.split('.').collect::<Vec<_>>());
        // ponytail: rebuild postings on delete; keep per-path reverse postings only if deletes are hot.
        if self.mode == IndexMode::Trigram {
            rebuild_trigrams(&mut state);
        }
        state.revisions.remove(metric);
        state.generation = state.generation.wrapping_add(1);
        true
    }

    pub fn find(&self, query: &str, limit: usize) -> Result<Vec<Match>, GlobError> {
        let glob = Glob::new(query)?;
        let state = self.state.read().expect("index lock poisoned");
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut matched = Vec::new();
        let trigrams = if self.mode == IndexMode::Trigram {
            glob.trigrams()
        } else {
            vec![]
        };
        if trigrams.is_empty() {
            let prefix = glob.literal_prefix();
            if let Some(node) = state.trie.node(prefix) {
                node.find_matches(prefix, &glob, &mut matched, limit, &mut 0)?;
            }
        } else {
            if trigrams.iter().any(|t| !state.trigrams.contains_key(t)) {
                return Ok(matched);
            }
            let candidates = trigrams
                .iter()
                .filter_map(|t| state.trigrams.get(t))
                .min_by_key(|set| set.len())
                .unwrap();
            for path in candidates {
                if trigrams.iter().all(|t| state.trigrams[t].contains(path))
                    && glob.matches_checked(path, 100_000)?
                {
                    matched.push(Match {
                        path: path.clone(),
                        is_leaf: state.metrics.contains_key(path),
                    });
                    if matched.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(matched)
    }

    /// Atomically replaces a scan snapshot when no newer index update occurred.
    /// Callers retry their scan if this returns false, so arrivals are never lost.
    pub fn reconcile_if_generation<I>(&self, expected: u64, entries: I) -> bool
    where
        I: IntoIterator<Item = (String, MetricMeta)>,
    {
        let mut state = self.state.write().expect("index lock poisoned");
        if state.generation != expected {
            return false;
        }
        state.metrics = entries.into_iter().collect();
        state.trie = TrieNode::default();
        for metric in state.metrics.keys().cloned().collect::<Vec<_>>() {
            state.trie.insert(&metric);
        }
        if self.mode == IndexMode::Trigram {
            rebuild_trigrams(&mut state);
        }
        state.generation = state.generation.wrapping_add(1);
        let revision = state.generation;
        state.revisions = state
            .metrics
            .keys()
            .map(|name| (name.clone(), revision))
            .collect();
        true
    }
}

fn prefixes(metric: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for segment in metric.split('.') {
        if !current.is_empty() {
            current.push('.');
        }
        current.push_str(segment);
        out.push(current.clone());
    }
    out
}
fn rebuild_trigrams(state: &mut State) {
    state.trigrams.clear();
    for metric in state.metrics.keys() {
        for path in prefixes(metric) {
            for tri in path.as_bytes().windows(3) {
                state
                    .trigrams
                    .entry([tri[0], tri[1], tri[2]])
                    .or_default()
                    .insert(path.clone());
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn limits_apply_to_matches_not_prefiltered_candidates() {
        for mode in [IndexMode::Trie, IndexMode::Trigram] {
            let index = Index::new(mode);
            for n in 0..2000 {
                index.upsert(&format!("a.metric{n:04}.no"), MetricMeta::default());
            }
            index.upsert("z.metric.yes", MetricMeta::default());
            assert_eq!(
                index.find("*.metric*.yes", 1).unwrap(),
                vec![Match {
                    path: "z.metric.yes".into(),
                    is_leaf: true
                }]
            );
            if mode == IndexMode::Trie {
                assert!(index.state.read().unwrap().trigrams.is_empty());
            }
            let first = index.snapshot().last().unwrap().2;
            index.upsert("z.metric.yes", MetricMeta::default());
            assert_ne!(index.snapshot().last().unwrap().2, first);
        }
    }
    fn index(mode: IndexMode) -> Index {
        let i = Index::new(mode);
        for n in [
            "a.b",
            "a.b.c",
            "a.x",
            "service.frontend.random.404.xoxo.http",
        ] {
            i.upsert(n, MetricMeta::default());
        }
        i
    }
    #[test]
    fn glob_features() {
        assert!(
            Glob::new("service.frontend.{random-404_xoxo,random.404.xoxo}.http*")
                .unwrap()
                .matches("service.frontend.random.404.xoxo.http")
        );
        assert!(Glob::new("a.[bx]").unwrap().matches("a.b"));
        assert!(!Glob::new("a.*").unwrap().matches("a.b.c"));
        assert!(Glob::new("a.*.*").unwrap().matches("a.b.c"));
        assert!(Glob::new("a.[^x]").unwrap().matches("a.b"));
        assert!(Glob::new("a.[").is_err());
    }
    #[test]
    fn modes_agree_and_leaf_can_be_branch() {
        for mode in [IndexMode::Trie, IndexMode::Trigram] {
            let got = index(mode).find("a.*", 10).unwrap();
            assert_eq!(
                got,
                vec![
                    Match {
                        path: "a.b".into(),
                        is_leaf: true
                    },
                    Match {
                        path: "a.x".into(),
                        is_leaf: true
                    }
                ]
            );
        }
    }
    #[test]
    fn updates_delete_and_reconcile() {
        let i = index(IndexMode::Trie);
        assert!(i.remove("a.b"));
        assert_eq!(
            i.find("a.b", 10).unwrap(),
            vec![Match {
                path: "a.b".into(),
                is_leaf: false
            }]
        );
        let g = i.generation();
        i.upsert("later.metric", MetricMeta::default());
        assert!(!i.reconcile_if_generation(g, [("scan.metric".into(), MetricMeta::default())]));
    }
    #[test]
    fn trigram_does_not_filter_brace_alternatives() {
        let i = Index::new(IndexMode::Trigram);
        i.upsert("service.bar.tail", MetricMeta::default());
        assert_eq!(
            i.find("service.{bar,baz}.tail", 10).unwrap(),
            vec![Match {
                path: "service.bar.tail".into(),
                is_leaf: true
            }]
        );
    }
    #[test]
    fn trie_limit_bounds_broad_queries() {
        let i = Index::new(IndexMode::Trie);
        for n in 0..2_000 {
            i.upsert(&format!("metric.{n:04}"), MetricMeta::default());
        }
        assert_eq!(i.find("metric.*", 1).unwrap().len(), 1);
    }
}
