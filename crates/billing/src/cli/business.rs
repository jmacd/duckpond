// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `accounts business set` -- one-row company identity table.

use crate::cli::Result;
use crate::schema::{BUSINESS_PATH, BusinessRow};
use crate::store;
use tinyfs::WD;

pub async fn set(
    wd: &WD,
    name: String,
    address: String,
    contact: String,
    ein: Option<String>,
) -> Result<()> {
    let row = BusinessRow {
        name,
        address,
        contact,
        ein,
    };
    log::info!(
        "[OK] business set: name={} address={} contact={} ein={:?}",
        row.name,
        row.address.replace('\n', " | "),
        row.contact,
        row.ein
    );
    // business.parquet is always exactly one row; overwrite unconditionally.
    store::write_table(wd, BUSINESS_PATH, &[row]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_overwrites_single_row() {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");

        set(&wd, "Caspar".into(), "addr1".into(), "p:111".into(), None)
            .await
            .expect("set v1");
        let rows: Vec<BusinessRow> = store::read_table(&wd, BUSINESS_PATH).await.expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "Caspar");

        set(
            &wd,
            "Caspar Water".into(),
            "addr2".into(),
            "p:222".into(),
            Some("00-0000000".into()),
        )
        .await
        .expect("set v2");
        let rows: Vec<BusinessRow> = store::read_table(&wd, BUSINESS_PATH).await.expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "Caspar Water");
        assert_eq!(rows[0].ein.as_deref(), Some("00-0000000"));
    }
}
