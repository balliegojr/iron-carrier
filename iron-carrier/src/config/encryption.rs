//! Handles configuration

use serde::{de::Visitor, Deserialize};

#[derive(Default, Debug, PartialEq, Eq)]
pub enum Encryption {
    #[default]
    Enabled,
    EnabledWithKey(String),
    Disabled,
}

impl Encryption {
    pub fn is_enabled(&self) -> bool {
        matches!(self, Encryption::Enabled | Encryption::EnabledWithKey(..))
    }

    pub fn encryption_key(&self) -> Option<&str> {
        if let Encryption::EnabledWithKey(ref key) = self {
            Some(key.as_str())
        } else {
            None
        }
    }
}

impl<'de> Deserialize<'de> for Encryption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(EncryptionVisitor)
    }
}
struct EncryptionVisitor;
impl<'de> Visitor<'de> for EncryptionVisitor {
    type Value = Encryption;

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v {
            Ok(Encryption::Enabled)
        } else {
            Ok(Encryption::Disabled)
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Encryption::EnabledWithKey(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Encryption::EnabledWithKey(v.to_string()))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Expecting boolean or string")
    }
}

#[cfg(test)]
mod tests {

    use crate::{config::Config, validation::Unvalidated};

    use super::*;

    #[test]
    fn can_parse_encryption_properly() -> anyhow::Result<()> {
        let config: Unvalidated<Config> = r#"
        [storages]
        "#
        .to_owned()
        .parse()?;
        assert_eq!(config.encryption, Encryption::Enabled);

        let config: Unvalidated<Config> = r#"
        encryption = false
        [storages]
        "#
        .to_owned()
        .parse()?;
        assert_eq!(config.encryption, Encryption::Disabled);

        let config: Unvalidated<Config> = r#"
        encryption = "some key"
        [storages]
        "#
        .to_owned()
        .parse()?;
        assert_eq!(
            config.encryption,
            Encryption::EnabledWithKey("some key".to_string())
        );

        Ok(())
    }
}
