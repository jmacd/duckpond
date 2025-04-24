use crate::glob::Error as GlobError;

use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Error>;
					 
/// Represents errors that can occur in filesystem operations
#[derive(Debug, PartialEq)]
pub enum Error {
    NotFound(PathBuf),
    NotADirectory(PathBuf),
    NotAFile(PathBuf),
    NotASymlink(PathBuf),
    PrefixNotSupported(PathBuf),
    RootPathFromNonRoot(PathBuf),
    ParentPathInvalid(PathBuf),
    EmptyPath,
    AlreadyExists(PathBuf),
    SymlinkLoop(PathBuf),
    GlobError(GlobError),
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

    pub fn empty_path() -> Self {
        Error::EmptyPath
    }

    pub fn already_exists<P: AsRef<Path>>(path: P) -> Self {
        Error::AlreadyExists(path.as_ref().to_path_buf())
    }

    pub fn symlink_loop<P: AsRef<Path>>(path: P) -> Self {
        Error::SymlinkLoop(path.as_ref().to_path_buf())
    }
}

impl From<GlobError> for Error {
    fn from(ge: GlobError) -> Error {
	Error::GlobError(ge)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::NotFound(path) => write!(f, "Path not found: {}", path.display()),
            Error::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
            Error::NotASymlink(path) => write!(f, "Not a symlink: {}", path.display()),
            Error::NotAFile(path) => write!(f, "Not a file: {}", path.display()),
            Error::PrefixNotSupported(path) => {
                write!(f, "Path prefix not supported: {}", path.display())
            }
            Error::RootPathFromNonRoot(path) => {
                write!(f, "Can't resolve root path: {}", path.display())
            }
            Error::ParentPathInvalid(path) => {
                write!(f, "Parent path invalid: {}", path.display())
            }
            Error::EmptyPath => write!(f, "Path is empty"),
            Error::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
            Error::SymlinkLoop(path) => write!(f, "Too many symbolic links: {}", path.display()),
            Error::GlobError(ge) => write!(f, "Bad glob expression: {:?}", ge),
        }
    }
}

impl std::error::Error for Error {}
