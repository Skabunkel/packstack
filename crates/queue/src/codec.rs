use crate::Result;

/// Turns a message into bytes for publishing.
pub trait Encode {
    fn encode(&self) -> Result<Vec<u8>>;
}

/// Builds a message from received bytes.
pub trait Decode: Sized {
    fn decode(bytes: &[u8]) -> Result<Self>;
}

impl Encode for String {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
}

impl Decode for String {
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

impl Encode for Vec<u8> {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }
}

impl Decode for Vec<u8> {
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

/// JSON encoded message body.
#[cfg(feature = "json")]
#[derive(Debug, Clone, Default)]
pub struct Json<T>(pub T);

#[cfg(feature = "json")]
impl<T: serde::Serialize> Encode for Json<T> {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self.0)?)
    }
}

#[cfg(feature = "json")]
impl<T: serde::de::DeserializeOwned> Decode for Json<T> {
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Json(serde_json::from_slice(bytes)?))
    }
}
