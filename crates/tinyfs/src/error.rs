use crate::glob::Error as GlobError;
use crate::file::Error as FileError;
use crate::dir::Error as DirError;
use crate::symlink::Error as SymlinkError;

use std::path::{Path, PathBuf};

/// Represents errors that can occur in filesystem operations
#[derive(Debug, PartialEq)]
pub enum FSError {
    NotFound(PathBuf),
    NotADirectory(PathBuf),
    NotAFile(PathBuf),
    PrefixNotSupported(PathBuf),
    RootPathFromNonRoot(PathBuf),
    ParentPathInvalid(PathBuf),
    EmptyPath,
    AlreadyExists(PathBuf),
    SymlinkLoop(PathBuf),
    GlobError(GlobError),
    DirError(DirError),
    FileError(FileError),
    SymlinkError(SymlinkError),
}

impl FSError {
    pub fn not_found<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotFound(path.as_ref().to_path_buf())
    }

    pub fn not_a_directory<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotADirectory(path.as_ref().to_path_buf())
    }

    pub fn not_a_file<P: AsRef<Path>>(path: P) -> Self {
        FSError::NotAFile(path.as_ref().to_path_buf())
    }

    pub fn prefix_not_supported<P: AsRef<Path>>(path: P) -> Self {
        FSError::PrefixNotSupported(path.as_ref().to_path_buf())
    }

    pub fn root_path_from_non_root<P: AsRef<Path>>(path: P) -> Self {
        FSError::RootPathFromNonRoot(path.as_ref().to_path_buf())
    }

    pub fn parent_path_invalid<P: AsRef<Path>>(path: P) -> Self {
        FSError::ParentPathInvalid(path.as_ref().to_path_buf())
    }

    pub fn empty_path() -> Self {
        FSError::EmptyPath
    }

    pub fn already_exists<P: AsRef<Path>>(path: P) -> Self {
        FSError::AlreadyExists(path.as_ref().to_path_buf())
    }

    pub fn symlink_loop<P: AsRef<Path>>(path: P) -> Self {
        FSError::SymlinkLoop(path.as_ref().to_path_buf())
    }
}

impl From<GlobError> for FSError {
    fn from(ge: GlobError) -> FSError {
	FSError::GlobError(ge)
    }
}

impl From<DirError> for FSError {
    fn from(de: DirError) -> FSError {
	FSError::DirError(de)
    }
}

impl From<FileError> for FSError {
    fn from(fe: FileError) -> FSError {
	FSError::FileError(fe)
    }
}

impl From<SymlinkError> for FSError {
    fn from(fe: SymlinkError) -> FSError {
	FSError::SymlinkError(fe)
    }
}

impl std::fmt::Display for FSError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FSError::NotFound(path) => write!(f, "Path not found: {}", path.display()),
            FSError::NotADirectory(path) => write!(f, "Not a directory: {}", path.display()),
            FSError::NotAFile(path) => write!(f, "Not a file: {}", path.display()),
            FSError::PrefixNotSupported(path) => {
                write!(f, "Path prefix not supported: {}", path.display())
            }
            FSError::RootPathFromNonRoot(path) => {
                write!(f, "Can't resolve root path: {}", path.display())
            }
            FSError::ParentPathInvalid(path) => {
                write!(f, "Parent path invalid: {}", path.display())
            }
            FSError::EmptyPath => write!(f, "Path is empty"),
            FSError::AlreadyExists(path) => write!(f, "Entry already exists: {}", path.display()),
            FSError::SymlinkLoop(path) => write!(f, "Too many symbolic links: {}", path.display()),
            FSError::GlobError(ge) => write!(f, "Bad glob expression: {:?}", ge),
            FSError::DirError(de) => write!(f, "Directory error: {:?}", de),
            FSError::FileError(de) => write!(f, "File error: {:?}", de),
            FSError::SymlinkError(de) => write!(f, "Symlink error: {:?}", de),
        }
    }
}

impl std::error::Error for FSError {}
