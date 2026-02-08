// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Markdown rendering and shortcode expansion for sitegen.
//!
//! Uses pulldown-cmark directly for markdown → HTML conversion, and a simple
//! `{{ name key="value" /}}` shortcode system for template expansion.
//!
//! ## Why not maudit?
//!
//! We originally used maudit's `render_markdown` but it added 800+ lines of
//! syntect highlighting and component abstractions we don't need.  Going
//! direct with pulldown-cmark gives us control over the parser options and
//! a smaller dependency footprint.

use pulldown_cmark::{Options, Parser, html::push_html};
use std::collections::HashMap;

// ─── Markdown rendering ──────────────────────────────────────────────────────

/// Render markdown to HTML.
///
/// Uses pulldown-cmark with GFM extensions (tables, strikethrough, task lists).
/// Raw HTML blocks (`<script>`, `<style>`, `<div>`, etc.) pass through
/// unchanged per the CommonMark spec.
pub fn render_markdown(content: &str) -> String {
    let options = Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_TABLES;

    let parser = Parser::new_ext(content, options);
    let mut html = String::with_capacity(content.len() * 2);
    push_html(&mut html, parser);
    html
}

// ─── Shortcodes ──────────────────────────────────────────────────────────────

/// A registered shortcode function.
pub type ShortcodeFn = Box<dyn Fn(&ShortcodeArgs) -> String + Send + Sync>;

/// Registry of named shortcode functions.
#[derive(Default)]
pub struct Shortcodes(HashMap<String, ShortcodeFn>);

impl Shortcodes {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn register<F>(&mut self, name: &str, func: F)
    where
        F: Fn(&ShortcodeArgs) -> String + Send + Sync + 'static,
    {
        self.0.insert(name.to_string(), Box::new(func));
    }

    fn get(&self, name: &str) -> Option<&ShortcodeFn> {
        self.0.get(name)
    }
}

/// Parsed key=value arguments from a shortcode invocation.
pub struct ShortcodeArgs(HashMap<String, String>);

impl ShortcodeArgs {
    /// Get argument as raw string.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }
}

/// Expand `{{ name key="value" /}}` shortcodes in markdown source.
///
/// Supports:
/// - Self-closing: `{{ chart /}}`
/// - With args: `{{ nav_list collection="params" base="/params" /}}`
/// - Block: `{{ wrap }}...{{ /wrap }}`
/// - Escaped: `\{{ literal }}`
pub fn preprocess_shortcodes(
    content: &str,
    shortcodes: &Shortcodes,
    markdown_path: Option<&str>,
) -> Result<String, String> {
    let mut output = String::new();
    let mut rest = content;

    while let Some(start) = rest.find("{{") {
        // Escaped \{{ → literal
        if start > 0 && rest.as_bytes()[start - 1] == b'\\' {
            output.push_str(&rest[..start - 1]);
            output.push_str("{{");
            rest = &rest[start + 2..];
            continue;
        }

        output.push_str(&rest[..start]);

        let remaining = &rest[start + 2..];
        let Some(tag_end) = remaining.find("}}") else {
            output.push_str("{{");
            rest = remaining;
            continue;
        };

        let sc_content = remaining[..tag_end].trim();
        let is_self_closing = sc_content.ends_with('/');
        let sc_content = if is_self_closing {
            sc_content.trim_end_matches('/').trim()
        } else {
            sc_content
        };

        // Parse name
        let mut parts = sc_content.split_whitespace();
        let name = parts.next().ok_or("Empty shortcode")?;

        if name.starts_with('/') {
            return Err(format!("Unexpected closing tag: {}", name));
        }

        if !is_valid_name(name) {
            output.push_str("{{");
            rest = remaining;
            continue;
        }

        // Parse key="value" arguments
        let args_str = parts.collect::<Vec<_>>().join(" ");
        let mut args = parse_args(&args_str)?;
        args.insert(
            "markdown_path".to_string(),
            markdown_path.unwrap_or("").to_string(),
        );

        let after_tag = &remaining[tag_end + 2..];

        if is_self_closing {
            if let Some(func) = shortcodes.get(name) {
                output.push_str(&func(&ShortcodeArgs(args)));
            } else {
                return Err(format!("Unknown shortcode: '{}'", name));
            }
            rest = after_tag;
        } else {
            // Block shortcode — find closing tag
            let close_compact = format!("{{{{/{}}}}}", name);
            let close_spaced = format!("{{{{ /{} }}}}", name);

            let close_pos = after_tag
                .find(&close_compact)
                .or_else(|| after_tag.find(&close_spaced));

            if let Some(pos) = close_pos {
                let close_len = if after_tag[pos..].starts_with(&close_compact) {
                    close_compact.len()
                } else {
                    close_spaced.len()
                };

                let body =
                    preprocess_shortcodes(&after_tag[..pos], shortcodes, markdown_path)?;
                let mut block_args = args;
                block_args.insert("body".to_string(), body);

                if let Some(func) = shortcodes.get(name) {
                    output.push_str(&func(&ShortcodeArgs(block_args)));
                } else {
                    return Err(format!("Unknown shortcode: '{}'", name));
                }
                rest = &after_tag[pos + close_len..];
            } else {
                return Err(format!(
                    "Block shortcode '{}' missing closing tag. Use '{{{{ {} /}}}}' for self-closing.",
                    name, name
                ));
            }
        }
    }

    output.push_str(rest);
    Ok(output)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Valid shortcode name: `[A-Za-z_][A-Za-z0-9_]+`
fn is_valid_name(name: &str) -> bool {
    if name.len() < 2 {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse `key="value" key2="value2"` into a HashMap.
fn parse_args(input: &str) -> Result<HashMap<String, String>, String> {
    let mut args = HashMap::new();
    if input.is_empty() {
        return Ok(args);
    }

    let mut chars = input.chars().peekable();
    let mut key = String::new();
    let mut value = String::new();
    let mut in_key = true;
    let mut in_quotes = false;
    let mut quote_char = '"';

    while let Some(ch) = chars.next() {
        match ch {
            '=' if in_key && !in_quotes => {
                in_key = false;
                if let Some(&q) = chars.peek() {
                    if q == '"' || q == '\'' {
                        quote_char = q;
                        in_quotes = true;
                        chars.next();
                    }
                }
            }
            c if !in_key && in_quotes && c == quote_char => {
                in_quotes = false;
                args.insert(key.trim().to_string(), value.clone());
                key.clear();
                value.clear();
                in_key = true;
                // skip trailing whitespace
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
            }
            ' ' if !in_quotes => {
                if !in_key && !value.is_empty() {
                    args.insert(key.trim().to_string(), value.trim().to_string());
                    key.clear();
                    value.clear();
                    in_key = true;
                }
                while chars.peek() == Some(&' ') {
                    chars.next();
                }
            }
            '\\' if in_quotes => {
                if let Some(esc) = chars.next() {
                    match esc {
                        '"' | '\'' | '\\' => value.push(esc),
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        _ => {
                            value.push('\\');
                            value.push(esc);
                        }
                    }
                }
            }
            _ => {
                if in_key {
                    key.push(ch);
                } else {
                    value.push(ch);
                }
            }
        }
    }

    if !in_key && !value.is_empty() {
        if in_quotes {
            return Err("Unclosed quote in argument value".to_string());
        }
        args.insert(key.trim().to_string(), value.trim().to_string());
    }

    Ok(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_markdown_basic() {
        let html = render_markdown("# Hello\n\nWorld");
        assert!(html.contains("<h1>Hello</h1>"));
        assert!(html.contains("<p>World</p>"));
    }

    #[test]
    fn test_render_markdown_script_passthrough() {
        let input = r#"# Title

<script type="module">
const x = 1;

items.forEach(i => {
  console.log(`Item ${i}`);
});
</script>
"#;
        let html = render_markdown(input);
        assert!(html.contains("<script type=\"module\">"));
        assert!(html.contains("items.forEach(i =>"));
        assert!(!html.contains("<p>items"));
        assert!(!html.contains("<code>"));
    }

    #[test]
    fn test_shortcode_self_closing() {
        let mut sc = Shortcodes::new();
        sc.register("hello", |_args: &ShortcodeArgs| "HELLO".to_string());

        let result = preprocess_shortcodes("before {{ hello /}} after", &sc, None);
        assert_eq!(result.unwrap(), "before HELLO after");
    }

    #[test]
    fn test_shortcode_with_args() {
        let mut sc = Shortcodes::new();
        sc.register("greet", |args: &ShortcodeArgs| {
            format!("Hi {}", args.get_str("name").unwrap_or("?"))
        });

        let result = preprocess_shortcodes("{{ greet name=\"World\" /}}", &sc, None);
        assert_eq!(result.unwrap(), "Hi World");
    }

    #[test]
    fn test_shortcode_block() {
        let mut sc = Shortcodes::new();
        sc.register("wrap", |args: &ShortcodeArgs| {
            format!("<div>{}</div>", args.get_str("body").unwrap_or(""))
        });

        let result = preprocess_shortcodes("{{ wrap }}content{{ /wrap }}", &sc, None);
        assert_eq!(result.unwrap(), "<div>content</div>");
    }

    #[test]
    fn test_parse_args() {
        let args = parse_args(r#"collection="params" base="/params""#).unwrap();
        assert_eq!(args.get("collection").unwrap(), "params");
        assert_eq!(args.get("base").unwrap(), "/params");
    }
}
