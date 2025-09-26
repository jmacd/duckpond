use std::any::Any;
use std::path::PathBuf;
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
pub struct TemplateCollection {
    pub in_pattern: String,      // Glob pattern with wildcard expressions
    pub out_pattern: String,     // Output basename with $0, $1 placeholders (no '/' chars)
    pub template: Option<String>, // Inline template content
    pub template_file: Option<String>, // Path to template file (host filesystem)
}

// Use TemplateCollection directly as the spec (one collection per mknod)
pub type TemplateSpec = TemplateCollection;

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

    /// Discover template files using the in_pattern
    async fn discover_template_files(&self) -> TinyFSResult<Vec<(PathBuf, Vec<String>)>> {
        let pattern = &self.config.in_pattern;
        info!("TemplateDirectory::discover_template_files - scanning pattern {pattern}");

        let mut template_files = Vec::new();

        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        // Use collect_matches to find template files with the given pattern
        match fs.root().await?.collect_matches(&pattern).await {
            Ok(matches) => {
                for (node_path, captured) in matches {
                    let path_str = node_path.path.to_string_lossy().to_string();
                    info!("TemplateDirectory::discover_template_files - found match {path_str} with captures: {:?}", captured);
                    template_files.push((PathBuf::from(path_str), captured));
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
        
        // Discover template files from in_pattern
        let template_matches = self.discover_template_files().await?;
        
        // Check if any expanded out_pattern matches the requested filename
        for (_template_path, captured) in template_matches {
            let expanded_name = self.expand_out_pattern(&captured);
            
            if expanded_name == filename {
                info!("TemplateDirectory::get - found matching template file {filename}");
                
                // Get template content
                let template_content = self.get_template_content()?;
                
                // Create template file that will render content
                let template_file = TemplateFile::new(
                    template_content,
                    self.context.clone()
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
        
        // Discover template files from in_pattern
        let template_matches = self.discover_template_files().await?;
        
        let mut entries = Vec::new();
        
        for (_template_path, captured) in template_matches {
            let expanded_name = self.expand_out_pattern(&captured);
            
            info!("TemplateDirectory::entries - creating entry {expanded_name} from pattern match");
            
            // Get template content
            let template_content = self.get_template_content()?;
            
            let template_file = TemplateFile::new(
                template_content,
                self.context.clone()
            );

            let node_ref = tinyfs::NodeRef::new(Arc::new(Mutex::new(tinyfs::Node {
                id: tinyfs::NodeID::generate(),
                node_type: tinyfs::NodeType::File(template_file.create_handle()),
            })));

            entries.push(Ok((expanded_name, node_ref)));
        }
        
        let entries_len = entries.len();
        info!("TemplateDirectory::entries - returning {entries_len} entries");
        Ok(Box::pin(stream::iter(entries)))
    }
}

impl TemplateDirectory {
    /// Expand out_pattern with captured placeholders ($0, $1, etc.)
    fn expand_out_pattern(&self, captured: &[String]) -> String {
        let mut result = self.config.out_pattern.clone();
        
        for (i, capture) in captured.iter().enumerate() {
            let placeholder = format!("${}", i + 1);  // Use 1-based indexing
            result = result.replace(&placeholder, capture);
        }
        
        result
    }

    /// Get template content from either inline template or template_file
    fn get_template_content(&self) -> TinyFSResult<String> {
        if let Some(template) = &self.config.template {
            Ok(template.clone())
        } else {
            Err(tinyfs::Error::Other("No template content available - should have been resolved during validation".to_string()))
        }
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
    context: FactoryContext,
}

impl TemplateFile {
    pub fn new(template_content: String, context: FactoryContext) -> Self {
        Self {
            template_content,
            context,
        }
    }

    /// Create a FileHandle from this template file
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    async fn get_rendered_content(&self) -> TinyFSResult<String> {
        // Create Tera context and register built-in functions
        let mut tera = Tera::default();
        let mut context = TeraContext::new();

        // Debug: log template variables from FactoryContext
        log::debug!("Template variables from FactoryContext: {:?}", self.context.template_variables);

        // Add all template variables directly to context (structured format)
        // This includes "vars" for CLI variables and "export" for export data
        for (key, value) in &self.context.template_variables {
            context.insert(key, value);
        }

        // Add export data from previous export stage (if available) - fallback if not in template_variables
        if let Some(export_data) = &self.context.export_data {
            context.insert("export", export_data);
            log::debug!("Added export data to template context: {:?}", export_data);
        }

        // Debug: log template content and context
        log::debug!("Template content: {}", self.template_content);
        log::debug!("Template context: {:?}", self.context.template_variables);

        // Render template with built-in functions and variables available
        let rendered = tera.render_str(&self.template_content, &context)
            .map_err(|e| {
                log::error!("Template render error: {}", e);
                tinyfs::Error::Other(format!("Template render error: {}", e))
            })?;

        log::debug!("Rendered template result: {}", rendered);
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
    let mut spec: TemplateSpec = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid template spec: {}", e)))?;
    
    // Resolve template_file to template content at mknod time
    if let Some(template_file) = spec.template_file.clone() {
        if spec.template.is_some() {
            return Err(tinyfs::Error::Other("Cannot specify both 'template' and 'template_file'".to_string()));
        }
        
        log::debug!("Reading template file: {}", template_file);
        let template_content = std::fs::read_to_string(&template_file)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read template file '{}': {}", template_file, e)))?;
        
        // Move content to template field and clear template_file
        spec.template = Some(template_content);
        spec.template_file = None;
        
        log::debug!("Template file '{}' loaded and cleared", template_file);
    }
    
    // Ensure we have template content
    if spec.template.is_none() {
        return Err(tinyfs::Error::Other("Must specify either 'template' or 'template_file'".to_string()));
    }
    
    // Validate out_pattern doesn't contain '/' (should be basename only)
    if spec.out_pattern.contains('/') {
        return Err(tinyfs::Error::Other("out_pattern must be a basename (no '/' characters allowed)".to_string()));
    }
    
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