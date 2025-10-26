//! Reusable banner formatting for pond commands
//!
//! Provides utilities to format boxed headers with left- and right-justified fields

/// Standard box width for banners
pub const BANNER_WIDTH: usize = 64;

/// Format a boxed banner with a title
pub fn format_banner_title(title: &str) -> String {
    let mut output = String::new();
    output.push_str("╔════════════════════════════════════════════════════════════════╗\n");
    output.push_str(&format!("║ {:<62} ║\n", title));
    output.push_str("╚════════════════════════════════════════════════════════════════╝\n");
    output
}

/// Format a boxed banner with multiple lines of left/right justified content
/// 
/// Each line can have:
/// - Just left content
/// - Just right content (right-justified)
/// - Both left and right content on the same line
/// 
/// # Arguments
/// * `title` - Optional title for the banner
/// * `lines` - Vector of (Option<left>, Option<right>) tuples
pub fn format_banner_with_fields(
    title: Option<&str>,
    lines: Vec<(Option<String>, Option<String>)>,
) -> String {
    let mut output = String::new();
    
    // Top border
    output.push_str("╔════════════════════════════════════════════════════════════════╗\n");
    
    // Title line if provided
    if let Some(title_text) = title {
        output.push_str(&format!("║ {:<62} ║\n", title_text));
    }
    
    // Content lines
    for (left_opt, right_opt) in lines {
        match (left_opt, right_opt) {
            (Some(left), Some(right)) => {
                // Both left and right on same line
                let content_width = BANNER_WIDTH - 2; // Account for borders
                let total_text_len = left.len() + right.len();
                
                if total_text_len >= content_width {
                    // Text too long - just show left, truncated
                    let max_len = content_width - 3; // Leave room for "..."
                    if left.len() > max_len {
                        output.push_str(&format!("║ {}... ║\n", &left[..max_len]));
                    } else {
                        output.push_str(&format!("║ {:<62} ║\n", left));
                    }
                } else {
                    let spaces = content_width - total_text_len;
                    output.push_str(&format!("║ {}{:width$}{} ║\n", left, "", right, width = spaces));
                }
            }
            (Some(left), None) => {
                // Only left, left-justified
                output.push_str(&format!("║ {:<62} ║\n", left));
            }
            (None, Some(right)) => {
                // Only right, right-justified
                output.push_str(&format!("║ {:>62} ║\n", right));
            }
            (None, None) => {
                // Empty line
                output.push_str("║                                                                ║\n");
            }
        }
    }
    
    // Bottom border
    output.push_str("╚════════════════════════════════════════════════════════════════╝\n");
    
    output
}

/// Format a boxed banner with generic iterators of left and right content
/// 
/// The banner will have as many lines as the longer of the two iterators.
/// Items are displayed left-justified and right-justified on the same line when both exist.
/// 
/// # Arguments
/// * `title` - Optional title for the banner
/// * `left_items` - Iterator of items to display on the left (will be converted to String)
/// * `right_items` - Iterator of items to display on the right (will be converted to String)
/// 
/// # Example
/// ```
/// use utilities::banner::format_banner_from_iters;
/// 
/// let left = vec!["Pond abc123", "Created 2024-01-01"];
/// let right = vec!["jmacd", "workstation.local"];
/// let banner = format_banner_from_iters(None, left, right);
/// ```
pub fn format_banner_from_iters<L, R, LI, RI>(
    title: Option<&str>,
    left_items: LI,
    right_items: RI,
) -> String
where
    L: ToString,
    R: ToString,
    LI: IntoIterator<Item = L>,
    RI: IntoIterator<Item = R>,
{
    let mut left_iter = left_items.into_iter();
    let mut right_iter = right_items.into_iter();
    
    let mut lines = Vec::new();
    
    loop {
        let left_opt = left_iter.next().map(|item| item.to_string());
        let right_opt = right_iter.next().map(|item| item.to_string());
        
        // Stop when both iterators are exhausted
        if left_opt.is_none() && right_opt.is_none() {
            break;
        }
        
        lines.push((left_opt, right_opt));
    }
    
    format_banner_with_fields(title, lines)
}

/// Format transaction header banner (for show detailed mode)
/// 
/// # Arguments
/// * `label` - Label for the transaction (e.g., "Transaction")
/// * `sequence` - Transaction sequence number
/// * `uuids` - List of UUIDs associated with this transaction
pub fn format_transaction_banner(label: &str, sequence: i64, uuids: Vec<String>) -> String {
    let mut lines = Vec::new();
    
    if uuids.is_empty() {
        // No UUIDs - just label and number
        lines.push((Some(label.to_string()), None));
        lines.push((Some(sequence.to_string()), None));
    } else if uuids.len() == 1 {
        // One UUID - label shares with first UUID, number on second line alone
        lines.push((Some(label.to_string()), Some(uuids[0].clone())));
        lines.push((Some(sequence.to_string()), None));
    } else {
        // Multiple UUIDs - label shares with first, number shares with second
        lines.push((Some(label.to_string()), Some(uuids[0].clone())));
        lines.push((Some(sequence.to_string()), Some(uuids[1].clone())));
        
        // Remaining UUIDs, right-justified
        for uuid in uuids.iter().skip(2) {
            lines.push((None, Some(uuid.clone())));
        }
    }
    
    format_banner_with_fields(None, lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_banner_title() {
        let output = format_banner_title("TEST TITLE");
        assert!(output.contains("TEST TITLE"));
        assert!(output.contains("╔"));
        assert!(output.contains("╚"));
    }
    
    #[test]
    fn test_banner_with_both_sides() {
        let lines = vec![
            (Some("Left".to_string()), Some("Right".to_string())),
        ];
        let output = format_banner_with_fields(None, lines);
        assert!(output.contains("Left"));
        assert!(output.contains("Right"));
    }
    
    #[test]
    fn test_banner_from_iters() {
        let left = vec!["Line 1 left", "Line 2 left"];
        let right = vec!["Line 1 right", "Line 2 right"];
        let output = format_banner_from_iters(None, left, right);
        assert!(output.contains("Line 1 left"));
        assert!(output.contains("Line 1 right"));
        assert!(output.contains("Line 2 left"));
        assert!(output.contains("Line 2 right"));
    }
    
    #[test]
    fn test_banner_from_iters_uneven() {
        let left = vec!["Line 1", "Line 2", "Line 3"];
        let right = vec!["Right 1"];
        let output = format_banner_from_iters(None, left, right);
        assert!(output.contains("Line 1"));
        assert!(output.contains("Line 2"));
        assert!(output.contains("Line 3"));
        assert!(output.contains("Right 1"));
    }
    
    #[test]
    fn test_banner_overflow() {
        // Test that very long text doesn't cause panic
        let left = vec![
            "Pond 019a21a0-6d63-7586-ab35-9ba310908a7f", // 42 chars
            "Created 2025-10-26 17:45:53 UTC",           // 31 chars
        ];
        let right = vec![
            "jmacd",              // 5 chars
            "Mac.localdomain",    // 15 chars
        ];
        // First line: 42 + 5 = 47 chars (fits in 62)
        // Second line: 31 + 15 = 46 chars (fits in 62)
        let output = format_banner_from_iters(None, left.clone(), right.clone());
        assert!(output.contains("019a21a0-6d63-7586-ab35-9ba310908a7f"));
        assert!(output.contains("jmacd"));
        assert!(output.contains("Mac.localdomain"));
        
        // Test with truly overflowing text
        let very_long_left = "A".repeat(70);
        let output2 = format_banner_with_fields(None, vec![
            (Some(very_long_left.clone()), Some("Right".to_string()))
        ]);
        // Should not panic and should contain truncation
        assert!(output2.contains("..."));
    }
    
    #[test]
    fn test_transaction_banner_no_uuids() {
        let output = format_transaction_banner("Transaction", 42, vec![]);
        assert!(output.contains("Transaction"));
        assert!(output.contains("42"));
    }
    
    #[test]
    fn test_transaction_banner_with_uuids() {
        let uuids = vec![
            "019a1efb-7a09-7a75-bc76-e9669c915fc2".to_string(),
            "019a1efb-7a0a-7a75-bc76-e9669c915fc3".to_string(),
        ];
        let output = format_transaction_banner("Transaction", 42, uuids);
        assert!(output.contains("Transaction"));
        assert!(output.contains("42"));
        assert!(output.contains("019a1efb-7a09-7a75-bc76-e9669c915fc2"));
        assert!(output.contains("019a1efb-7a0a-7a75-bc76-e9669c915fc3"));
    }
}
