// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

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

use crate::FactoryContext;
use tinyfs::{
    AsyncReadSeek, Directory, EntryType, FS, File, FileHandle, Node, NodeMetadata,
    Result as TinyFSResult,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateCollection {
    pub in_pattern: crate::Url,    // URL pattern with wildcard expressions
    pub out_pattern: String,       // Output basename with $0, $1 placeholders (no '/' chars)
    pub template_file: crate::Url, // URL to template file WITHIN POND (e.g., "file:///templates/data.md.tmpl") - REQUIRED
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
}

impl TemplateDirectory {
    #[must_use]
    pub fn new(config: TemplateSpec, context: FactoryContext) -> Self {
        Self {
            config,
            context,
        }
    }

    /// Discover template files using the in_pattern
    async fn discover_template_files(&self) -> TinyFSResult<Vec<(PathBuf, Vec<String>)>> {
        let pattern = self.config.in_pattern.path();
        debug!("TemplateDirectory::discover_template_files - scanning pattern {pattern}");

        let mut template_files = Vec::new();

        let fs = FS::from_arc(self.context.context.persistence.clone());

        // Use collect_matches to find template files with the given pattern
        match fs.root().await?.collect_matches(pattern).await {
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

        // Discover template files from in_pattern
        let template_matches = self.discover_template_files().await?;

        // Build hierarchical export structure from ALL matches
        // This allows templates to access the full dataset for navigation/aggregation
        let matches_export = self.build_matches_export(&template_matches);

        // Check literal out_pattern FIRST - these templates don't use captures
        // (e.g., nav.html, index.html that aggregate all matches)
        if self.is_literal_out_pattern() && self.config.out_pattern == filename {
            debug!(
                "TemplateDirectory::get - creating literal template file {filename} with args=[]"
            );
            return self
                .create_template_node(filename, vec![], matches_export)
                .await;
        }

        // Check if any expanded out_pattern matches the requested filename
        // (e.g., param=$0.html expands to param=DO.html for captures ["DO", ...])
        for (_template_path, captured) in &template_matches {
            let expanded_name = self.expand_out_pattern(captured);

            if expanded_name == filename {
                debug!("TemplateDirectory::get - found matching template file {filename}");

                return self
                    .create_template_node(filename, captured.clone(), matches_export)
                    .await;
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

    async fn remove(&mut self, _name: &str) -> TinyFSResult<Option<Node>> {
        Err(tinyfs::Error::Other(
            "Template directories are read-only".to_string(),
        ))
    }

    async fn entries(
        &self,
    ) -> TinyFSResult<Pin<Box<dyn Stream<Item = TinyFSResult<tinyfs::DirectoryEntry>> + Send>>>
    {
        debug!("TemplateDirectory::entries - listing discovered template files");

        // Discover template files from in_pattern
        let template_matches = self.discover_template_files().await?;

        let mut entries = Vec::new();
        let mut seen_names = std::collections::HashSet::new();

        for (_template_path, captured) in &template_matches {
            let expanded_name = self.expand_out_pattern(captured);

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
                        EntryType::FileDynamic,
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
                    let error_msg =
                        format!("Failed to create template file '{}': {}", expanded_name, e);
                    error!("TemplateDirectory::entries - {error_msg}");
                    entries.push(Err(tinyfs::Error::Other(error_msg)));
                }
            }
        }

        // If out_pattern is a literal (no placeholders), add it even if there were no matches
        if self.is_literal_out_pattern() && !seen_names.contains(&self.config.out_pattern) {
            let literal_name = self.config.out_pattern.clone();
            debug!(
                "TemplateDirectory::entries - adding literal entry {literal_name} (no pattern matches required)"
            );

            match self.get(&literal_name).await {
                Ok(Some(node_ref)) => {
                    let dir_entry = tinyfs::DirectoryEntry::new(
                        literal_name,
                        node_ref.id().node_id(),
                        EntryType::FileDynamic,
                        0,
                    );
                    entries.push(Ok(dir_entry));
                }
                Ok(None) => {
                    let error_msg = format!("Failed to create literal template file '{}'", literal_name);
                    error!("TemplateDirectory::entries - {error_msg}");
                    entries.push(Err(tinyfs::Error::Other(error_msg)));
                }
                Err(e) => {
                    let error_msg =
                        format!("Failed to create literal template file '{}': {}", literal_name, e);
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
    /// Check if out_pattern is a literal (no $0, $1, etc. placeholders)
    fn is_literal_out_pattern(&self) -> bool {
        !self.config.out_pattern.contains('$')
    }

    /// Expand out_pattern with captured placeholders ($0, $1, etc.)
    fn expand_out_pattern(&self, captured: &[String]) -> String {
        let mut result = self.config.out_pattern.clone();

        for (i, capture) in captured.iter().enumerate() {
            let placeholder = format!("${}", i); // Use 0-based indexing for $0, $1, $2, etc.
            result = result.replace(&placeholder, capture);
        }

        result
    }

    /// Create a template file node with the given arguments and export context
    async fn create_template_node(
        &self,
        filename: &str,
        args: Vec<String>,
        matches_export: Value,
    ) -> TinyFSResult<Option<Node>> {
        // Get template content
        let template_content = self.get_template_content().await?;

        // Create template file that will render content
        // Pass the full export context so templates can access all matches
        let template_file = TemplateFile::new(
            template_content,
            self.context.clone(),
            args,
            matches_export,
        );

        // Generate deterministic FileID for this template file
        let mut id_bytes = Vec::new();
        id_bytes.extend_from_slice(filename.as_bytes());
        id_bytes.extend_from_slice(self.config.in_pattern.to_string().as_bytes());
        id_bytes.extend_from_slice(self.config.out_pattern.as_bytes());
        id_bytes.extend_from_slice(self.config.template_file.to_string().as_bytes());
        // Use this template directory's NodeID as the PartID for children
        let parent_part_id = tinyfs::PartID::from_node_id(self.context.file_id.node_id());
        let file_id =
            tinyfs::FileID::from_content(parent_part_id, EntryType::FileDynamic, &id_bytes);

        let node_ref = Node::new(
            file_id,
            tinyfs::NodeType::File(template_file.create_handle()),
        );

        Ok(Some(node_ref))
    }

    /// Build hierarchical export structure from pattern matches
    /// Groups matches by their capture structure (e.g., param/site → name → resolution)
    fn build_matches_export(
        &self,
        matches: &[(PathBuf, Vec<String>)],
    ) -> Value {
        use serde_json::Map;

        // Build nested structure from captures
        // Captures are hierarchical: e.g., ["param", "DO", "res=1h"] or ["site", "FieldStation", "res=1h"]
        let mut root: Map<String, Value> = Map::new();

        for (path, captures) in matches {
            if captures.is_empty() {
                continue;
            }

            // Navigate/create the nested structure
            let mut current = &mut root;

            for (i, capture) in captures.iter().enumerate() {
                if i == captures.len() - 1 {
                    // Leaf level - store the file path
                    let file_entry = Value::Object({
                        let mut m = Map::new();
                        _ = m.insert(
                            "file".to_string(),
                            Value::String(path.to_string_lossy().to_string()),
                        );
                        m
                    });

                    // If this key exists and is an object, merge; otherwise create array
                    if let Some(existing) = current.get_mut(capture) {
                        if let Value::Array(arr) = existing {
                            arr.push(file_entry);
                        } else {
                            // Convert to array
                            let prev = existing.take();
                            *existing = Value::Array(vec![prev, file_entry]);
                        }
                    } else {
                        _ = current.insert(capture.clone(), file_entry);
                    }
                } else {
                    // Intermediate level - ensure nested object exists
                    if !current.contains_key(capture) {
                        _ = current.insert(capture.clone(), Value::Object(Map::new()));
                    }

                    // Navigate into the nested object
                    if let Some(Value::Object(nested)) = current.get_mut(capture) {
                        current = nested;
                    } else {
                        // This shouldn't happen but handle gracefully
                        break;
                    }
                }
            }
        }

        Value::Object(root)
    }

    /// Get template content from template_file (pond path)
    async fn get_template_content(&self) -> TinyFSResult<String> {
        // Read template file from pond filesystem
        let fs = FS::from_arc(self.context.context.persistence.clone());

        let template_path = self.config.template_file.path();
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
            blake3: None,
            bao_outboard: None,
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
    /// Export context built from all in_pattern matches (hierarchical structure)
    matches_export: Value,
}

impl TemplateFile {
    #[must_use]
    pub fn new(
        template_content: String,
        context: FactoryContext,
        args: Vec<String>,
        matches_export: Value,
    ) -> Self {
        Self {
            template_content,
            context,
            args,
            matches_export,
        }
    }

    /// Create a FileHandle from this template file
    #[must_use]
    pub fn create_handle(self) -> FileHandle {
        FileHandle::new(Arc::new(Mutex::new(Box::new(self))))
    }

    async fn get_rendered_content(&self) -> TinyFSResult<String> {
        // Clone data needed for spawn_blocking (must be Send + 'static)
        let template_content = self.template_content.clone();
        let args = self.args.clone();
        let matches_export = self.matches_export.clone();
        let persistence = self.context.context.persistence.clone();
        
        // Get fresh template variables before entering blocking context
        let fresh_template_variables = self
            .context
            .context
            .template_variables
            .lock()
            .expect("template_variables lock not poisoned")
            .clone();
        debug!(
            "🎨 RENDER: Fresh template variables during rendering: {:?}",
            fresh_template_variables.keys().collect::<Vec<_>>()
        );

        // Run Tera rendering in spawn_blocking to allow sync functions to use block_on
        tokio::task::spawn_blocking(move || {
            // Get handle to current runtime for block_on calls inside Tera functions
            let handle = tokio::runtime::Handle::current();
            
            // Create Tera context and register built-in functions
            let mut tera = Tera::default();

            tera.register_function("group", tmpl_group);
            tera.register_filter("to_json", tmpl_to_json);
            tera.register_filter("keys", tmpl_keys);
            
            // Register insert_from_path function with access to persistence
            let persistence_for_fn = persistence.clone();
            let handle_for_fn = handle.clone();
            tera.register_function("insert_from_path", move |fn_args: &HashMap<String, Value>| {
                tmpl_insert_from_path(fn_args, &persistence_for_fn, &handle_for_fn)
            });
            
            debug!("Template functions registered successfully");

            let mut context = TeraContext::new();

            // Add all fresh template variables to context (vars, export, and any other keys)
            for (key, value) in fresh_template_variables {
                context.insert(&key, &value);
                debug!("🎨 RENDER: Added '{}' to template context", key);
            }

            // @@@ Terrible!
            let argsval = serde_json::value::to_value(&args)
                .map_err(|_| tinyfs::Error::Other(format!("could not json {:?}", args)))?;
            context.insert("args", &argsval);

            // Add export context:
            // - If args is empty (literal out_pattern like nav.html, index.html), use matches_export
            //   These templates build their own navigation context from pattern matching
            // - If args is non-empty (parameterized templates like param=$0.html), use pipeline's export
            //   These templates need the export context from the export command (files, schema, etc.)
            info!(
                "🎨 EXPORT DECISION: args.is_empty()={}, matches_export.is_null()={}, args={:?}",
                args.is_empty(),
                matches_export.is_null(),
                args
            );
            if args.is_empty() && !matches_export.is_null() {
                // Literal templates use their own pattern-matched context
                context.insert("export", &matches_export);
                info!(
                    "🎨 RENDER: Added 'export' from in_pattern matches (literal template)"
                );
            } else if !context.contains_key("export") && !matches_export.is_null() {
                // Fallback: use matches_export if pipeline didn't provide export
                context.insert("export", &matches_export);
                debug!(
                    "🎨 RENDER: Added 'export' from in_pattern matches (fallback): {:?}",
                    matches_export
                );
            } else {
                info!(
                    "🎨 RENDER: NOT overriding export - args non-empty or matches_export is null"
                );
            }

            debug!("🎨 RENDER: Template context ready");
            debug!("Template context setup complete - vars and export guaranteed available");
            debug!("Template content: {}", template_content);
            debug!("Template context has export data available");

            // Render template with built-in functions and variables available
            let rendered = tera
                .render_str(&template_content, &context)
                .map_err(|e| {
                    error!("=== TEMPLATE RENDER ERROR ===");
                    error!("Template content: {}", template_content);
                    info!("Template variables available in context: {:?}", context);

                    // Print complete error chain with proper traversal
                    error!("Error chain ({} levels):", count_error_chain_levels(&e));
                    print_error_chain(&e);

                    error!("=== END TEMPLATE ERROR ===");
                    tinyfs::Error::Other(format!("Template render error: {}", e))
                })?;

            debug!("Rendered template result: {}", rendered);
            Ok(rendered)
        }).await.map_err(|e| tinyfs::Error::Other(format!("spawn_blocking failed: {}", e)))?
    }
}

#[async_trait]
impl File for TemplateFile {
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn AsyncReadSeek>>> {
        let content = self.get_rendered_content().await?;
        let cursor = std::io::Cursor::new(content.into_bytes());
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> TinyFSResult<Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
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
            blake3: None,
            bao_outboard: None,
            entry_type: EntryType::FileDynamic,
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

    let template_dir = TemplateDirectory::new(spec, context);
    Ok(template_dir.create_handle())
}

/// Validate template configuration and return processed spec as Value
fn validate_template_config(config: &[u8]) -> TinyFSResult<Value> {
    let spec: TemplateSpec = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid template spec: {}", e)))?;

    // Validate template_file is a pond path (starts with /)
    debug!("Validating template file path: {}", spec.template_file);
    let template_path = spec.template_file.path();
    if !template_path.starts_with('/') {
        return Err(tinyfs::Error::Other(format!(
            "Template file '{}' must be an absolute pond path (e.g., 'file:///templates/data.md.tmpl')",
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

/// Insert content from a pond path
/// Usage: {{ insert_from_path(path="/templates/nav/nav.html") }}
/// 
/// This function reads a file from the pond filesystem and returns its content.
/// The file at the path can be a dynamic template file, which will be rendered
/// before its content is returned.
fn tmpl_insert_from_path(
    args: &HashMap<String, Value>,
    persistence: &Arc<dyn tinyfs::PersistenceLayer>,
    handle: &tokio::runtime::Handle,
) -> Result<Value, Error> {
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::msg("insert_from_path requires 'path' argument"))?;

    debug!("insert_from_path: reading path '{}'", path);

    // Use block_on to run async file read - safe because we're in spawn_blocking
    let persistence = persistence.clone();
    let path = path.to_string();
    let path_for_debug = path.clone();
    
    let content = handle.block_on(async move {
        use tokio::io::AsyncReadExt;
        
        let fs = FS::from_arc(persistence);
        let root = fs.root().await.map_err(|e| {
            Error::msg(format!("insert_from_path: failed to get root: {}", e))
        })?;
        
        let mut reader = root.async_reader_path(&path).await.map_err(|e| {
            Error::msg(format!("insert_from_path: failed to open '{}': {}", path, e))
        })?;
        
        let mut content = String::new();
        let _ = reader.read_to_string(&mut content).await.map_err(|e| {
            Error::msg(format!("insert_from_path: failed to read '{}': {}", path, e))
        })?;
        
        Ok::<String, Error>(content)
    })?;

    debug!("insert_from_path: read {} bytes from '{}'", content.len(), path_for_debug);
    Ok(Value::String(content))
}

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

/// Get keys of an object as a sorted array
/// Usage: {% for key in obj | keys %}
fn tmpl_keys(value: &Value, _: &HashMap<String, Value>) -> Result<Value, Error> {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            Ok(Value::Array(keys.into_iter().map(Value::String).collect()))
        }
        _ => Err(Error::msg("keys filter requires an object")),
    }
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
