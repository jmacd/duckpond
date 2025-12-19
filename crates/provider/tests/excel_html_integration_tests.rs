// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod excel_html_integration_tests {
    use datafusion::prelude::*;
    use provider::Provider;
    use std::sync::Arc;
    use tinyfs::async_helpers::convenience;

    #[tokio::test]
    async fn test_excelhtml_hydrovu_file() -> Result<(), Box<dyn std::error::Error>> {
        // Initialize TinyFS with memory persistence
        let persistence = tinyfs::MemoryPersistence::default();
        let fs = Arc::new(tinyfs::FS::new(persistence).await?);

        // Copy HTML file into TinyFS
        let html_path = "/Volumes/sourcecode/src/duckpond/sample-excelhtml/HydroVu_FB-Bottom-664623_2024-12-31_22-00-00_Export.htm";
        if !std::path::Path::new(html_path).exists() {
            return Ok(());
        }

        let html_content = std::fs::read(html_path)?;
        let root = fs.root().await?;
        let _ = convenience::create_file_path(&root, "/hydrovu.htm", &html_content).await?;

        // Create provider and open table
        let provider = Provider::new(fs);
        let ctx = SessionContext::new();

        let table = provider
            .create_table_provider("excelhtml:///hydrovu.htm", &ctx)
            .await?;

        // Register and query
        let _ = ctx.register_table("hydrovu", table)?;

        // Count rows
        let count_df = ctx
            .sql("SELECT COUNT(*) as total_rows FROM hydrovu")
            .await?;
        let results = count_df.collect().await?;

        assert!(!results.is_empty());

        // Verify we have the expected columns
        let table_provider = ctx.table("hydrovu").await?;
        let schema_ref = table_provider.schema();
        let schema = schema_ref.as_arrow();
        assert!(schema.field_with_name("Date Time").is_ok());
        assert!(schema.field_with_name("Temperature (C)").is_ok());
        assert!(schema.field_with_name("Salinity (psu)").is_ok());

        // Sample query
        let sample_df = ctx.sql("SELECT \"Date Time\", \"Temperature (C)\", \"Salinity (psu)\" FROM hydrovu LIMIT 3").await?;
        let sample_results = sample_df.collect().await?;
        assert!(!sample_results.is_empty());

        Ok(())
    }
}
