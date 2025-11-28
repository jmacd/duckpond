use std::path::{Path, PathBuf};
use crate::node::FileID;

pub type Result<T> = std::result::Result<T, Error>;

/// Represents errors that can occur in filesystem operations
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("AlreadyExists: {0}")]
    AlreadyExists(PathBuf),
    #[error("EmptyPath")]
    EmptyPath,
    #[error("IDNotFound: {0}")]
    IDNotFound(FileID),
    #[error("Immutable: {0}")]
    Immutable(PathBuf),
    #[error("Internal: {0}")]
    Internal(String),
    #[error("InvalidComponent: {0}")]
    InvalidComponent(PathBuf),
    #[error("InvalidConfig: {0}")]
    InvalidConfig(String),
    #[error("MultipleWildcards: {0}")]
    MultipleWildcards(String),
    #[error("NotADirectory: {0}")]
    NotADirectory(PathBuf),
    #[error("NotAFile: {0}")]
    NotAFile(PathBuf),
    #[error("NotASymlink: {0}")]
    NotASymlink(PathBuf),
    #[error("NotFound: {0}")]
    NotFound(PathBuf),
    #[error("Other: {0}")]
    Other(String),
    #[error("ParentPathInvalid: {0}")]
    ParentPathInvalid(PathBuf),
    #[error("PrefixNotSupported: {0}")]
    PrefixNotSupported(PathBuf),
    #[error("RootPathFromNonRoot: {0}")]
    RootPathFromNonRoot(PathBuf),
    #[error("SymlinkLoop: {0}")]
    SymlinkLoop(PathBuf),
    #[error("VisitLoop: {0}")]
    VisitLoop(PathBuf),
}

impl Error {
    pub fn not_found<P: AsRef<Path>>(path: P) -> Self {
        Error::NotFound(path.as_ref().to_path_buf())
    }

    pub fn not_a_directory<P: AsRef<Path>>(path: P) -> Self {
        Error::NotADirectory(path.as_ref().to_path_buf())
    }

    pub fn not_a_symlink<P: AsRef<Path>>(path: P) -> Self {
        Error::NotASymlink(path.as_ref().to_path_buf())
    }

    pub fn not_a_file<P: AsRef<Path>>(path: P) -> Self {
        Error::NotAFile(path.as_ref().to_path_buf())
    }

    pub fn prefix_not_supported<P: AsRef<Path>>(path: P) -> Self {
        Error::PrefixNotSupported(path.as_ref().to_path_buf())
    }

    pub fn root_path_from_non_root<P: AsRef<Path>>(path: P) -> Self {
        Error::RootPathFromNonRoot(path.as_ref().to_path_buf())
    }

    pub fn parent_path_invalid<P: AsRef<Path>>(path: P) -> Self {
        Error::ParentPathInvalid(path.as_ref().to_path_buf())
    }

    #[must_use]
    pub fn empty_path() -> Self {
        Error::EmptyPath
    }

    pub fn immutable<P: AsRef<Path>>(path: P) -> Self {
        Error::Immutable(path.as_ref().to_path_buf())
    }

    pub fn already_exists<P: AsRef<Path>>(path: P) -> Self {
        Error::AlreadyExists(path.as_ref().to_path_buf())
    }

    pub fn symlink_loop<P: AsRef<Path>>(path: P) -> Self {
        Error::SymlinkLoop(path.as_ref().to_path_buf())
    }

    pub fn visit_loop<P: AsRef<Path>>(path: P) -> Self {
        Error::VisitLoop(path.as_ref().to_path_buf())
    }

    /// Create a MultipleWildcards error from a string-like value
    pub fn multiple_wildcards<S: AsRef<str>>(s: S) -> Self {
        Error::MultipleWildcards(s.as_ref().into())
    }

    /// Create an InvalidComponent error from a path-like value
    pub fn invalid_component<P: AsRef<Path>>(p: P) -> Self {
        Error::InvalidComponent(p.as_ref().into())
    }

    /// Create an InvalidComponent error from a path-like value
    pub fn internal<S: Into<String>>(s: S) -> Self {
        Error::Internal(s.into())
    }
}

// impl std::fmt::Display for Error {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
// 	    Error::IDNotFound(id) => write!(f, "ID not found: {id:?}"),
//             Error::NotFound(path) => write!(f, "Path not found: {}", path.display()),
//             Error::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
//             Error::NotASymlink(path) => write!(f, "Not a symlink: {}", path.display()),
//             Error::NotAFile(path) => write!(f, "Not a file: {}", path.display()),
//             Error::PrefixNotSupported(path) => {
//                 write!(f, "Path prefix not supported: {}", path.display())
//             }
//             Error::RootPathFromNonRoot(path) => {
//                 write!(f, "Can't resolve root path: {}", path.display())
//             }
//             Error::ParentPathInvalid(path) => {
//                 write!(f, "Parent path invalid: {}", path.display())
//             }
//             Error::Immutable(path) => {
//                 write!(f, "Immutable path: {}", path.display())
//             }
//             Error::EmptyPath => write!(f, "Path is empty"),
//             Error::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
//             Error::SymlinkLoop(path) => write!(f, "Too many symbolic links: {}", path.display()),
//             Error::VisitLoop(path) => write!(f, "Recursive visit to self: {}", path.display()),
//             Error::Other(msg) => write!(f, "Error: {}", msg),
//             Error::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
//             Error::MultipleWildcards(part) => write!(f, "Multiple wildcards: {}", part),
//             Error::InvalidComponent(path) => write!(f, "Invalid component: {}", path.display()),
//             Error::Internal(msg) => write!(f, "Internal error: {}", msg),
//         }
//     }
// }
