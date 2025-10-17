use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use log::info;

/// Recover pond from potential crash state
///
/// This command operates on potentially damaged ponds and doesn't use transaction guards.
/// The recovery process itself handles internal transactions through Ship::recover().
pub async fn recover_command(ship_context: &ShipContext) -> Result<()> {
    let pond_path = ship_context.resolve_pond_path()?;
    let pond_path_display = format!("{}", pond_path.display());
    info!("Starting recovery process for pond: {pond_path_display}");

    // Attempt to open the pond - this may fail if the pond is in an inconsistent state
    let mut ship = ship_context
        .open_pond()
        .await
        .map_err(|e| anyhow!("Failed to open pond for recovery: {}", e))?;

    // Perform recovery - Ship::recover() handles all the internal logic
    let recovery_result = ship
        .recover()
        .await
        .map_err(|e| anyhow!("Recovery failed: {}", e))?;

    // Report results
    if recovery_result.was_needed {
        let recovered_count = recovery_result.recovered_count;
        info!("✅ Recovery completed: {recovered_count} transaction(s) recovered");
    } else {
        info!("✅ No recovery needed - pond is consistent");
    }

    // Verify recovery was successful by checking if further recovery is needed
    ship.check_recovery_needed().await.map_err(|e| match e {
        steward::StewardError::RecoveryNeeded { .. } => {
            anyhow!("Recovery check failed - pond still needs recovery")
        }
        e => anyhow!("Post-recovery verification failed: {}", e),
    })?;

    info!("✅ Recovery command completed successfully");
    Ok(())
}
