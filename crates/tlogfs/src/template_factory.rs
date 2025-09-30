use async_trait::async_trait;
use futures::{stream, Stream};
use log::{info, error};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::path::PathBuf;
use std::pin::Pin;
use std::collections::HashMap;
use std::sync::Arc;
use tera::{Tera, Context as TeraContext, Error, Value, from_value};
use tokio::sync::Mutex;

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
    // Cache to ensure consistent NodeIDs for the same filename
    node_cache: Arc<Mutex<HashMap<String, NodeRef>>>,
}

impl TemplateDirectory {
    pub fn new(config: TemplateSpec, context: FactoryContext) -> Self {
        Self { 
            config, 
            context,
            node_cache: Arc::new(Mutex::new(HashMap::new())),
        }
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
        
        // Check cache first
        {
            let cache = self.node_cache.lock().await;
            if let Some(node_ref) = cache.get(filename) {
                info!("TemplateDirectory::get - returning cached node for {filename}");
                return Ok(Some(node_ref.clone()));
            }
        }
        
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
                    self.context.clone(),
		    captured,
                );

                let node_ref = tinyfs::NodeRef::new(Arc::new(Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(template_file.create_handle()),
                })));

                // Cache the node
                {
                    let mut cache = self.node_cache.lock().await;
                    cache.insert(filename.to_string(), node_ref.clone());
                }

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
        let mut seen_names = std::collections::HashSet::new();
        
        for (_template_path, captured) in template_matches {
            let expanded_name = self.expand_out_pattern(&captured);
            
            // Skip if we've already created this output file
            if seen_names.contains(&expanded_name) {
                continue;
            }
            seen_names.insert(expanded_name.clone());
            
            info!("TemplateDirectory::entries - creating entry {expanded_name} from pattern match");
            
            // Check cache first
            let node_ref = {
                let cache = self.node_cache.lock().await;
                cache.get(&expanded_name).cloned()
            };
            
            let node_ref = if let Some(cached_node) = node_ref {
                info!("TemplateDirectory::entries - using cached node for {expanded_name}");
                cached_node
            } else {
                info!("TemplateDirectory::entries - creating new node for {expanded_name}");
                
                // Get template content
                let template_content = self.get_template_content()?;
                
                let template_file = TemplateFile::new(
                    template_content,
                    self.context.clone(),
                    captured,
                );

                let new_node_ref = tinyfs::NodeRef::new(Arc::new(Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(template_file.create_handle()),
                })));

                // Cache the node
                {
                    let mut cache = self.node_cache.lock().await;
                    cache.insert(expanded_name.clone(), new_node_ref.clone());
                }

                new_node_ref
            };

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
            let placeholder = format!("${}", i);  // Use 0-based indexing for $0, $1, $2, etc.
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
    args: Vec<String>,
}

impl TemplateFile {
    pub fn new(template_content: String, context: FactoryContext, args: Vec<String>) -> Self {
        Self {
            template_content,
            context,
	    args,
        }
    }

    /// Create a FileHandle from this template file
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    async fn get_rendered_content(&self) -> TinyFSResult<String> {
        // Create Tera context and register built-in functions
        let mut tera = Tera::default();

        tera.register_function("group", tmpl_group);
        tera.register_filter("to_json", tmpl_to_json);
        log::debug!("to_json filter registered successfully");
        
        let mut context = TeraContext::new();

        // Get FRESH template variables from state during rendering (not cached context)
        // This ensures we see export data added after factory creation
        let fresh_template_variables = (*self.context.state.get_template_variables()).clone();
        log::info!("🎨 RENDER: Fresh template variables during rendering: {:?}", fresh_template_variables.keys().collect::<Vec<_>>());
        
        // Add all fresh template variables to context (vars, export, and any other keys)
        for (key, value) in fresh_template_variables {
            context.insert(&key, &value);
            log::info!("🎨 RENDER: Added '{}' to template context", key);
        }

	// @@@ Terrible!
	let argsval = serde_json::value::to_value(&self.args)
	    .map_err(|_| tinyfs::Error::Other(format!("could not json {:?}", self.args)))?;
	context.insert("args", &argsval);
        
        // DO NOT add empty export - let template fail if export is missing
        // This forces us to fix the timing issue instead of hiding it
        log::info!("🎨 RENDER: Template context ready - no fallback for missing export data");

        log::debug!("Template context setup complete - vars and export guaranteed available");

        // Debug: log template content and context
        log::debug!("Template content: {}", self.template_content);
        log::debug!("Template context has export data available");

        // Render template with built-in functions and variables available
        let rendered = tera.render_str(&self.template_content, &context)
            .map_err(|e| {
                log::error!("=== TEMPLATE RENDER ERROR ===");
                log::error!("Template content: {}", self.template_content);
                log::error!("Template variables available in context: {:?}", context);
                log::error!("Tera error: {}", e);
                log::error!("Error kind: {:?}", e.kind);
                log::error!("=== END TEMPLATE ERROR ===");
                tinyfs::Error::Other(format!("Template render error: {} (kind: {:?})", e, e.kind))
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
        // Don't render template during metadata calls to avoid context issues
        // Template files show as 0 bytes until actually accessed
        Ok(NodeMetadata {
            version: 1,
            size: Some(0),  // Placeholder size - will be accurate when file is read
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

/// Validate template configuration and return processed spec as Value
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


fn tmpl_group(args: &HashMap<String, Value>) -> Result<Value, Error> {
    let key = args
        .get("by")
        .map(|x| from_value::<String>(x.clone()))
        .ok_or_else(|| Error::msg("missing group-by key"))??;

    let obj = args
        .get("in")
        .ok_or_else(|| Error::msg("expected a input value"))?;

    match obj {
        Value::Array(items) => {
            let mut mapped = serde_json::Map::new();
            // For each input item in an array
            for item in items.into_iter() {
                match item.clone() {
                    // Expect the item is an object.
                    Value::Object(fields) => {
                        // Expect the value is a string
                        let idk = fields
                            .get(&key)
                            .ok_or_else(|| Error::msg("expected a string-valued group"))?;
                        let value = from_value::<String>(idk.clone())?;

                        // Check mapped.get(value)
                        mapped
                            .entry(&value)
                            .and_modify(|e| {
                                e.as_array_mut().expect("is an array").push(item.clone())
                            })
                            .or_insert_with(|| serde_json::json!(vec![item.clone()]));
                    }
                    _ => {
                        return Err(Error::msg("cannot group non-object"));
                    }
                }
            }
            Ok(Value::Object(mapped))
        }
        _ => Err(Error::msg("cannot group non-array")),
    }
}

fn tmpl_to_json(value: &tera::Value, _: &std::collections::HashMap<String, tera::Value>) -> Result<Value, Error> {
    log::debug!("to_json filter called with value: {:?}", value);
    let json_string = serde_json::to_string_pretty(value).unwrap_or_else(|e| {
        log::error!("Failed to serialize value to JSON: {}", e);
        "null".to_string()
    });
    log::debug!("to_json filter returning: {}", json_string);
    Ok(tera::Value::String(json_string))
}
