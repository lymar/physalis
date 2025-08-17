use std::sync::Arc;

/// Идентификатор Aggregate
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Identity {
    pub namespace: Namespace,
    pub id: Id,
}

/// Неймспейс агрегата, например, `user`. Не должен содержать символ `/`.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Namespace(Arc<str>);

/// Идентификатор агрегата, например, `123`. Не должен быть пустой строкой.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Id(Arc<str>);

#[derive(thiserror::Error, Debug)]
pub enum IdentityError {
    #[error("wrong namespace format")]
    WrongNamespace(#[from] NamespaceError),
    #[error("wrong id format")]
    WrongId(#[from] IdError),
}

#[derive(thiserror::Error, Debug)]
pub enum NamespaceError {
    #[error("wrong namespace format (contains '/')")]
    WrongNamespaceFormat,
}

#[derive(thiserror::Error, Debug)]
pub enum IdError {
    #[error("wrong id format (empty string)")]
    WrongIdFormat,
}

impl Identity {
    pub fn new(
        namespace: impl Into<Arc<str>>,
        id: impl Into<Arc<str>>,
    ) -> Result<Self, IdentityError> {
        Ok(Self {
            namespace: Namespace::new(namespace)?,
            id: Id::new(id)?,
        })
    }

    pub fn key(&self) -> String {
        format!("{}/{}", self.namespace, self.id)
    }
}

mod namespace {
    use std::sync::Arc;

    use super::{Namespace, NamespaceError};

    impl Namespace {
        pub fn new(
            namespace: impl Into<Arc<str>>,
        ) -> Result<Self, NamespaceError> {
            let s = namespace.into();
            if s.contains('/') {
                Err(NamespaceError::WrongNamespaceFormat)
            } else {
                Ok(Self(s))
            }
        }
    }

    impl std::fmt::Display for Namespace {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::ops::Deref for Namespace {
        type Target = str;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
}

mod id {
    use std::sync::Arc;

    use super::{Id, IdError};

    impl Id {
        pub fn new(id: impl Into<Arc<str>>) -> Result<Self, IdError> {
            let s = id.into();
            if s.is_empty() {
                Err(IdError::WrongIdFormat)
            } else {
                Ok(Self(s))
            }
        }
    }

    impl std::ops::Deref for Id {
        type Target = str;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl std::fmt::Display for Id {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use super::*;

    #[test]
    fn test_namespace() -> anyhow::Result<()> {
        let ns = Namespace::new("test")?;
        assert_eq!(ns.deref(), "test");

        let ns = Namespace::new("test/asd");
        assert!(matches!(ns, Err(NamespaceError::WrongNamespaceFormat)));

        Ok(())
    }

    #[test]
    fn test_id() -> anyhow::Result<()> {
        let id = Id::new("test")?;
        assert_eq!(id.deref(), "test");

        let id = Id::new("");
        assert!(matches!(id, Err(IdError::WrongIdFormat)));

        Ok(())
    }

    #[test]
    fn test_identity() -> anyhow::Result<()> {
        let ns = Namespace::new("some.ns")?;
        let id = Id::new("id".to_string())?;
        let identity = Identity { namespace: ns, id };

        assert_eq!(identity.key(), "some.ns/id");

        Ok(())
    }
}
