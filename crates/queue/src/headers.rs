use async_nats::HeaderMap;

/// Message headers. Names compare case-insensitively, one value per name.
#[derive(Debug, Clone, Default)]
pub struct Headers(Vec<(String, String)>);

impl Headers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a header, replacing any existing value with the same name.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let (name, value) = (name.into(), value.into());
        match self.0.iter_mut().find(|(n, _)| n.eq_ignore_ascii_case(&name)) {
            Some(entry) => entry.1 = value,
            None => self.0.push((name, value)),
        }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }

    /// Inserts every header from `other`, overriding existing values.
    pub fn extend(&mut self, other: Headers) {
        for (name, value) in other.0 {
            self.insert(name, value);
        }
    }

    pub(crate) fn to_nats(&self) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in &self.0 {
            map.insert(name.as_str(), value.as_str());
        }
        map
    }

    pub(crate) fn from_nats(map: Option<&HeaderMap>) -> Self {
        let mut headers = Self::new();
        for (name, values) in map.into_iter().flat_map(HeaderMap::iter) {
            if let Some(value) = values.last() {
                headers.insert(name.to_string(), value.as_str());
            }
        }
        headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_replaces_case_insensitive() {
        let mut headers = Headers::new();
        headers.insert("X-Trace", "a");
        headers.insert("x-trace", "b");
        assert_eq!(headers.get("X-TRACE"), Some("b"));
        assert_eq!(headers.iter().count(), 1);
    }

    #[test]
    fn nats_round_trip() {
        let mut headers = Headers::new();
        headers.insert("x-service", "packstack");
        let back = Headers::from_nats(Some(&headers.to_nats()));
        assert_eq!(back.get("x-service"), Some("packstack"));
    }
}
