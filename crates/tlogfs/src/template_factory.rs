use std::any::Any;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use tera::{Tera, Context as TeraContext};
use std::pin::Pin;
use futures::{stream, Stream};
use log::{info, error};
use serde_json::Value;

use tinyfs::{AsyncReadSeek, File, FileHandle, Result as TinyFSResult, NodeMetadata, EntryType, Directory, NodeRef, FS};
use crate::factory::FactoryContext;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSpec {
    /// Glob pattern for discovering template files (e.g., "/base/*.template")
    pub source: String,
    pub patterns: Vec<String>,
    pub template_content: String, // Template content with Tera syntax
}

#[derive(Clone)]
pub struct TemplateContext {
    pub variables: HashMap<String, String>,
}

pub struct TemplateFactory;

impl TemplateFactory {
    pub fn new() -> Self {
        Self
    }
}

/// Template directory that provides template file rendering
pub struct TemplateDirectory {
    config: TemplateSpec,
    context: FactoryContext,
}

impl TemplateDirectory {
    pub fn new(config: TemplateSpec, context: FactoryContext) -> Self {
        Self { config, context }
    }

    /// Discover template files using the source pattern
    async fn discover_template_files(&self) -> TinyFSResult<Vec<PathBuf>> {
        let pattern = &self.config.source;
        info!("TemplateDirectory::discover_template_files - scanning pattern {pattern}");

        let mut template_files = Vec::new();

        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        // Use collect_matches to find template files with the given pattern
        match fs.root().await?.collect_matches(&pattern).await {
            Ok(matches) => {
                for (node_path, _captured) in matches {
                    let path_str = node_path.path.to_string_lossy().to_string();
                    // Check if this file matches our template patterns
                    let filename = Path::new(&path_str).file_name()
                        .and_then(|f| f.to_str())
                        .unwrap_or("");
                    
                    if self.matches_template_pattern(filename) {
                        info!("TemplateDirectory::discover_template_files - found template file {path_str}");
                        template_files.push(PathBuf::from(path_str));
                    }
                }
            }
            Err(e) => {
                error!("TemplateDirectory::discover_template_files - failed to match pattern {pattern}: {}", e);
                return Err(tinyfs::Error::Other(format!("Failed to match template pattern: {}", e)));
            }
        }
        
        let count = template_files.len();
        info!("TemplateDirectory::discover_template_files - discovered {count} template files");
        Ok(template_files)
    }

    /// Create a DirHandle from this template directory
    pub fn create_handle(self) -> tinyfs::DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for TemplateDirectory {
    async fn get(&self, filename: &str) -> TinyFSResult<Option<NodeRef>> {
        info!("TemplateDirectory::get - looking for {filename}");
        
        // Discover template files from source
        let template_files = self.discover_template_files().await?;
        
        // Check if any discovered file matches the requested filename
        for template_path in template_files {
            let file_name = template_path.file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("");
                
            if file_name == filename {
                info!("TemplateDirectory::get - found matching template file {filename}");
                
                // Create template file that will render content from the source file
                let template_file = TemplateFile::new(
                    self.config.template_content.clone()
                );

                let node_ref = tinyfs::NodeRef::new(Arc::new(Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(template_file.create_handle()),
                })));

                return Ok(Some(node_ref));
            }
        }
        
        info!("TemplateDirectory::get - no matching template file found for {filename}");
        Ok(None)
    }

    async fn insert(&mut self, _filename: String, _node: NodeRef) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other("Template directories are read-only".to_string()))
    }

    async fn entries(&self) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<(String, NodeRef)>> + Send>>> {
        info!("TemplateDirectory::entries - listing discovered template files");
        
        // Discover template files from source
        let template_files = self.discover_template_files().await?;
        
        let mut entries = Vec::new();
        
        for template_path in template_files {
            let file_name = template_path.file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("");
                
            if !file_name.is_empty() {
                info!("TemplateDirectory::entries - creating entry {file_name} from template {}", template_path.display());
                
                let template_file = TemplateFile::new(
                    self.config.template_content.clone()
                );

                let node_ref = tinyfs::NodeRef::new(Arc::new(Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(template_file.create_handle()),
                })));

                entries.push(Ok((file_name.to_string(), node_ref)));
            }
        }
        
        let entries_len = entries.len();
        info!("TemplateDirectory::entries - returning {entries_len} entries");
        Ok(Box::pin(stream::iter(entries)))
    }
}

impl TemplateDirectory {
    /// Check if a filename matches any of our template patterns
    fn matches_template_pattern(&self, filename: &str) -> bool {
        self.config.patterns.iter().any(|pattern| {
            // Simple pattern matching - could be enhanced with glob later
            if pattern.contains('*') {
                let prefix = pattern.split('*').next().unwrap_or("");
                let suffix = pattern.split('*').last().unwrap_or("");
                filename.starts_with(prefix) && filename.ends_with(suffix)
            } else {
                filename == pattern
            }
        })
    }
}

#[async_trait]
impl tinyfs::Metadata for TemplateDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::Directory,
            timestamp: 0,
        })
    }
}

/// Template file that provides rendered content
pub struct TemplateFile {
    template_content: String,
}

impl TemplateFile {
    pub fn new(template_content: String) -> Self {
        Self {
            template_content,
        }
    }

    /// Create a FileHandle from this template file
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    async fn get_rendered_content(&self) -> TinyFSResult<String> {
        // Create Tera context and register built-in functions
        let mut tera = Tera::default();
        let context = TeraContext::new();

        // Render template with built-in functions available
        let rendered = tera.render_str(&self.template_content, &context)
            .map_err(|e| tinyfs::Error::Other(format!("Template render error: {}", e)))?;

        Ok(rendered)
    }
}

#[async_trait]
impl File for TemplateFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn AsyncReadSeek>>> {
        let content = self.get_rendered_content().await?;
        let cursor = std::io::Cursor::new(content.into_bytes());
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tokio::io::AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other("Template files are read-only".to_string()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl tinyfs::Metadata for TemplateFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        let content = self.get_rendered_content().await?;
        Ok(NodeMetadata {
            version: 1,
            size: Some(content.len() as u64),
            sha256: None,
            entry_type: EntryType::FileData,
            timestamp: 0,
        })
    }
}

/// Create template directory with context (factory function)
fn create_template_directory(
    config: Value,
    context: &FactoryContext,
) -> TinyFSResult<tinyfs::DirHandle> {
    let spec: TemplateSpec = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid template spec: {}", e)))?;

    let template_dir = TemplateDirectory::new(spec, context.clone());
    Ok(template_dir.create_handle())
}

/// Validate template configuration
fn validate_template_config(config: &[u8]) -> TinyFSResult<Value> {
    let spec: TemplateSpec = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid template spec: {}", e)))?;
    
    // Convert to JSON Value for internal processing
    let json_value = serde_json::to_value(spec)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config to JSON: {}", e)))?;
    
    Ok(json_value)
}

// Register the template factory
crate::register_dynamic_factory!(
    name: "template",
    description: "Create template files with Tera template engine",
    directory_with_context: create_template_directory,
    validate: validate_template_config
);