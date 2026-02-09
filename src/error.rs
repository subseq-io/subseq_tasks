use anyhow::Error as AnyError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Conflict,
    Database,
    Forbidden,
    InvalidInput,
    NotFound,
    Unknown,
}

#[derive(Debug)]
pub struct LibError {
    pub kind: ErrorKind,
    pub public: String,
    pub source: AnyError,
}

impl LibError {
    pub fn conflict<S: Into<String>>(public: S, source: AnyError) -> Self {
        Self {
            kind: ErrorKind::Conflict,
            public: public.into(),
            source,
        }
    }

    pub fn database<S: Into<String>>(public: S, source: AnyError) -> Self {
        Self {
            kind: ErrorKind::Database,
            public: public.into(),
            source,
        }
    }

    pub fn forbidden<S: Into<String>>(public: S, source: AnyError) -> Self {
        Self {
            kind: ErrorKind::Forbidden,
            public: public.into(),
            source,
        }
    }

    pub fn invalid<S: Into<String>>(public: S, source: AnyError) -> Self {
        Self {
            kind: ErrorKind::InvalidInput,
            public: public.into(),
            source,
        }
    }

    pub fn not_found<S: Into<String>>(public: S, source: AnyError) -> Self {
        Self {
            kind: ErrorKind::NotFound,
            public: public.into(),
            source,
        }
    }

    pub fn unknown<S: Into<String>>(public: S, source: AnyError) -> Self {
        Self {
            kind: ErrorKind::Unknown,
            public: public.into(),
            source,
        }
    }
}

pub type Result<T> = std::result::Result<T, LibError>;

impl From<subseq_graph::error::LibError> for LibError {
    fn from(value: subseq_graph::error::LibError) -> Self {
        let kind = match value.kind {
            subseq_graph::error::ErrorKind::Conflict => ErrorKind::Conflict,
            subseq_graph::error::ErrorKind::Database => ErrorKind::Database,
            subseq_graph::error::ErrorKind::Forbidden => ErrorKind::Forbidden,
            subseq_graph::error::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            subseq_graph::error::ErrorKind::NotFound => ErrorKind::NotFound,
            subseq_graph::error::ErrorKind::Unknown => ErrorKind::Unknown,
        };

        Self {
            kind,
            public: value.public.to_string(),
            source: value.source,
        }
    }
}
