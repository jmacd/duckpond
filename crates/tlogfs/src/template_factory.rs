use async_trait::async_trait;
use futures::{Stream, stream};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tera::{Context as TeraContext, Error, Tera, Value, from_value};
use tokio::sync::Mutex;

use crate::factory::FactoryContext;
use tinyfs::{
    AsyncReadSeek, Directory, EntryType, FS, File, FileHandle, NodeMetadata,
    Result as TinyFSResult, Node,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateCollection {
    pub in_pattern: String,    // Glob pattern with wildcard expressions
    pub out_pattern: String,   // Output basename with $0, $1 placeholders (no '/' chars)
    pub template_file: String, // Path to template file WITHIN POND (e.g., "/templates/data.md.tmpl") - REQUIRED
}

// Use TemplateCollection directly as the spec (one collection per mknod)
pub type TemplateSpec = TemplateCollection;

pub struct TemplateFactory;

impl Default for TemplateFactory {
    fn default() -> Self {
        Self
    }
}

/// Template directory that provides template file rendering
pub struct TemplateDirectory {
    config: TemplateSpec,
    context: FactoryContext,
    // Cache to ensure consistent NodeIDs for the same filename
    node_cache: Arc<Mutex<HashMap<String, Node>>>,
}

impl TemplateDirectory {
    #[must_use]
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
        debug!("TemplateDirectory::discover_template_files - scanning pattern {pattern}");

        let mut template_files = Vec::new();

        let fs = FS::new(self.context.state.clone())
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        // Use collect_matches to find template files with the given pattern
        match fs.root().await?.collect_matches(&pattern).await {
            Ok(matches) => {
                for (node_path, captured) in matches {
                    let path_str = node_path.path.to_string_lossy().to_string();
                    debug!(
                        "TemplateDirectory::discover_template_files - found match {path_str} with captures: {:?}",
                        captured
                    );
                    template_files.push((PathBuf::from(path_str), captured));
                }
            }
            Err(e) => {
                error!(
                    "TemplateDirectory::discover_template_files - failed to match pattern {pattern}: {}",
                    e
                );
                return Err(tinyfs::Error::Other(format!(
                    "Failed to match template pattern: {}",
                    e
                )));
            }
        }

        let count = template_files.len();
        debug!("TemplateDirectory::discover_template_files - discovered {count} template files");
        Ok(template_files)
    }

    /// Create a DirHandle from this template directory
    #[must_use]
    pub fn create_handle(self) -> tinyfs::DirHandle {
        tinyfs::DirHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }
}

#[async_trait]
impl Directory for TemplateDirectory {
    async fn get(&self, filename: &str) -> TinyFSResult<Option<Node>> {
        debug!("TemplateDirectory::get - looking for {filename}");

        // Check cache first
        {
            let cache = self.node_cache.lock().await;
            if let Some(node_ref) = cache.get(filename) {
                debug!("TemplateDirectory::get - returning cached node for {filename}");
                return Ok(Some(node_ref.clone()));
            }
        }

        // Discover template files from in_pattern
        let template_matches = self.discover_template_files().await?;

        // Check if any expanded out_pattern matches the requested filename
        for (_template_path, captured) in template_matches {
            let expanded_name = self.expand_out_pattern(&captured);

            if expanded_name == filename {
                debug!("TemplateDirectory::get - found matching template file {filename}");

                // Get template content
                let template_content = self.get_template_content().await?;

                // Create template file that will render content
                let template_file =
                    TemplateFile::new(template_content, self.context.clone(), captured);

                // Generate deterministic node_id for this template file
                let mut id_bytes = Vec::new();
                id_bytes.extend_from_slice(filename.as_bytes());
                id_bytes.extend_from_slice(self.config.in_pattern.as_bytes());
                id_bytes.extend_from_slice(self.config.out_pattern.as_bytes());
                id_bytes.extend_from_slice(self.config.template_file.as_bytes());
                let node_id = tinyfs::NodeID::from_content(&id_bytes);
                let file_id = tinyfs::FileID::new_from_ids(self.context.file_id.part_id(), node_id);

                let node_ref = Node::new(file_id, tinyfs::NodeType::File(template_file.create_handle()));

                // Cache the node
                {
                    let mut cache = self.node_cache.lock().await;
                    _ = cache.insert(filename.to_string(), node_ref.clone());
                }

                return Ok(Some(node_ref));
            }
        }

        debug!("TemplateDirectory::get - no matching template file found for {filename}");
        Ok(None)
    }

    async fn insert(&mut self, _filename: String, _node: Node) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other(
            "Template directories are read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>> {
        debug!("TemplateDirectory::entries - listing discovered template files");

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
            _ = seen_names.insert(expanded_name.clone());

            debug!(
                "TemplateDirectory::entries - creating entry {expanded_name} from pattern match"
            );

            // Template files need to be created/loaded to get their real node_id
            match self.get(&expanded_name).await {
                Ok(Some(node_ref)) => {
                    // Extract real node_id from the created/cached node
                    let dir_entry = tinyfs::DirectoryEntry::new(
                        expanded_name.clone(),
                        node_ref.id().node_id(),
                        EntryType::FileDataDynamic,
                        0,
                    );
                    entries.push(Ok(dir_entry));
                }
                Ok(None) => {
                    let error_msg = format!("Failed to create template file '{}'", expanded_name);
                    error!("TemplateDirectory::entries - {error_msg}");
                    entries.push(Err(tinyfs::Error::Other(error_msg)));
                }
                Err(e) => {
                    let error_msg = format!("Failed to create template file '{}': {}", expanded_name, e);
                    error!("TemplateDirectory::entries - {error_msg}");
                    entries.push(Err(tinyfs::Error::Other(error_msg)));
                }
            }
        }

        let entries_len = entries.len();
        debug!("TemplateDirectory::entries - returning {entries_len} entries");
        Ok(Box::pin(stream::iter(entries)))
    }
}

impl TemplateDirectory {
    /// Expand out_pattern with captured placeholders ($0, $1, etc.)
    fn expand_out_pattern(&self, captured: &[String]) -> String {
        let mut result = self.config.out_pattern.clone();

        for (i, capture) in captured.iter().enumerate() {
            let placeholder = format!("${}", i); // Use 0-based indexing for $0, $1, $2, etc.
            result = result.replace(&placeholder, capture);
        }

        result
    }

    /// Get template content from template_file (pond path)
    async fn get_template_content(&self) -> TinyFSResult<String> {
        // Read template file from pond filesystem
        let fs = FS::new(self.context.state.clone())
            .await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        let template_path = &self.config.template_file;
        debug!("Reading template file from pond: {}", template_path);

        // Read the template file from the pond
        use tokio::io::AsyncReadExt;
        let mut reader = fs
            .root()
            .await?
            .async_reader_path(template_path)
            .await
            .map_err(|e| {
                tinyfs::Error::Other(format!(
                    "Failed to open template file '{}' in pond: {}",
                    template_path, e
                ))
            })?;

        let mut contents = String::new();
        _ = reader.read_to_string(&mut contents).await.map_err(|e| {
            tinyfs::Error::Other(format!(
                "Failed to read template file '{}': {}",
                template_path, e
            ))
        })?;

        debug!("Template file loaded: {} bytes", contents.len());
        Ok(contents)
    }
}

#[async_trait]
impl tinyfs::Metadata for TemplateDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::DirectoryDynamic,
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
    #[must_use]
    pub fn new(template_content: String, context: FactoryContext, args: Vec<String>) -> Self {
        Self {
            template_content,
            context,
            args,
        }
    }

    /// Create a FileHandle from this template file
    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    async fn get_rendered_content(&self) -> TinyFSResult<String> {
        // Create Tera context and register built-in functions
        let mut tera = Tera::default();

        tera.register_function("group", tmpl_group);
        tera.register_filter("to_json", tmpl_to_json);
        debug!("to_json filter registered successfully");

        let mut context = TeraContext::new();

        // Get FRESH template variables from state during rendering (not cached context)
        // This ensures we see export data added after factory creation
        let fresh_template_variables = (*self.context.state.get_template_variables()).clone();
        debug!(
            "🎨 RENDER: Fresh template variables during rendering: {:?}",
            fresh_template_variables.keys().collect::<Vec<_>>()
        );

        // Add all fresh template variables to context (vars, export, and any other keys)
        for (key, value) in fresh_template_variables {
            context.insert(&key, &value);
            debug!("🎨 RENDER: Added '{}' to template context", key);
        }

        // @@@ Terrible!
        let argsval = serde_json::value::to_value(&self.args)
            .map_err(|_| tinyfs::Error::Other(format!("could not json {:?}", self.args)))?;
        context.insert("args", &argsval);

        // DO NOT add empty export - let template fail if export is missing
        // This forces us to fix the timing issue instead of hiding it
        debug!("🎨 RENDER: Template context ready - no fallback for missing export data");

        debug!("Template context setup complete - vars and export guaranteed available");

        // Debug: log template content and context
        debug!("Template content: {}", self.template_content);
        debug!("Template context has export data available");

        // Render template with built-in functions and variables available
        let rendered = tera
            .render_str(&self.template_content, &context)
            .map_err(|e| {
                error!("=== TEMPLATE RENDER ERROR ===");
                error!("Template content: {}", self.template_content);
                info!("Template variables available in context: {:?}", context);

                // Print complete error chain with proper traversal
                error!("Error chain ({} levels):", count_error_chain_levels(&e));
                print_error_chain(&e);

                error!("=== END TEMPLATE ERROR ===");
                tinyfs::Error::Other(format!("Template render error: {}", e))
            })?;

        debug!("Rendered template result: {}", rendered);
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
        Err(tinyfs::Error::Other(
            "Template files are read-only".to_string(),
        ))
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
            size: Some(0), // Placeholder size - will be accurate when file is read
            sha256: None,
            entry_type: EntryType::FileDataDynamic,
            timestamp: 0,
        })
    }
}

/// Create template directory with context (factory function)
fn create_template_directory(
    config: Value,
    context: FactoryContext,
) -> TinyFSResult<tinyfs::DirHandle> {
    let spec: TemplateSpec = from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid template spec: {}", e)))?;

    let template_dir = TemplateDirectory::new(spec, context.clone());
    Ok(template_dir.create_handle())
}

/// Validate template configuration and return processed spec as Value
fn validate_template_config(config: &[u8]) -> TinyFSResult<Value> {
    let spec: TemplateSpec = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid template spec: {}", e)))?;

    // Validate template_file is a pond path (starts with /)
    debug!("Validating template file path: {}", spec.template_file);
    if !spec.template_file.starts_with('/') {
        return Err(tinyfs::Error::Other(format!(
            "Template file '{}' must be an absolute pond path (e.g., '/templates/data.md.tmpl')",
            spec.template_file
        )));
    }

    // Note: We cannot validate file existence here because validation happens
    // during mknod BEFORE the factory is initialized with FactoryContext.
    // File existence is checked at runtime when get_template_content() is called.
    debug!("Template file path validated: {}", spec.template_file);

    // Validate out_pattern doesn't contain '/' (should be basename only)
    if spec.out_pattern.contains('/') {
        return Err(tinyfs::Error::Other(
            "out_pattern must be a basename (no '/' characters allowed)".to_string(),
        ));
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
    directory: create_template_directory,
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
            for item in items {
                match item.clone() {
                    // Expect the item is an object.
                    Value::Object(fields) => {
                        // Expect the value is a string
                        let idk = fields
                            .get(&key)
                            .ok_or_else(|| Error::msg("expected a string-valued group"))?;
                        let value = from_value::<String>(idk.clone())?;

                        // Check mapped.get(value)
                        _ = mapped
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

fn tmpl_to_json(value: &Value, _: &HashMap<String, Value>) -> Result<Value, Error> {
    debug!("to_json filter called with value: {:?}", value);
    let json_string = serde_json::to_string_pretty(value).unwrap_or_else(|e| {
        // EXPLICIT BUSINESS LOGIC: Template rendering should not fail due to JSON serialization
        // This is user-facing functionality where graceful degradation is appropriate
        error!(
            "Template JSON serialization failed: {} - returning null placeholder",
            e
        );
        "null".to_string()
    });
    debug!("to_json filter returning: {}", json_string);
    Ok(Value::String(json_string))
}

/// Count the number of levels in an error chain
fn count_error_chain_levels(err: &dyn std::error::Error) -> usize {
    let mut count = 1;
    let mut source = err.source();

    while let Some(err) = source {
        count += 1;
        source = err.source();
    }

    count
}

/// Print complete error chain with proper formatting (from Tera diagnostics report)
fn print_error_chain(err: &dyn std::error::Error) {
    error!("  → {}", err);

    let mut source = err.source();
    let mut level = 1;

    while let Some(err) = source {
        error!("  ├─ Level {}: {}", level, err);
        source = err.source();
        level += 1;
    }
}
