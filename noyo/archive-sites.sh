#!/bin/sh
# ARCHIVE HYDROVU INSTRUMENT DATA
#
# Exports raw instrument .series data from the pond as Parquet files
# for git archival.  This is the same pattern as noyo/laketech/ — raw
# instrument data checked into git and imported at setup time.
#
# Instruments to archive (no longer collecting):
#   BDockVulink    - Moved to Field Station 10/17/2025
#   BDockAT500     - Moved to Field Station 10/17/2025
#   SilverVulink1  - Failed ~May 30, 2024
#   SilverVulink2  - Replaced SilverVulink1; site decommissioned
#   SilverAT500    - Site decommissioned
#   PrincessVulink - Moved to Field Station 10/17/2025
#   PrincessAT500  - Moved to Field Station 10/17/2025
#
# The factory layer (combine.yaml) handles mapping instruments → sites
# with time ranges and scope prefixes.  That layer doesn't change — it
# continues to reference the same pond paths whether the data came from
# the HydroVu API or from archived Parquet imports.
#
# After running this script:
#   1. git add noyo/hydrovu/*.parquet
#   2. Update hydrovu.yaml to mark instruments inactive
#   3. Update setup.sh to import archived Parquet at pond init time
#   4. Verify: rebuild pond, check /combined/* still works

set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond

export POND

OUTDIR=${NOYO}/hydrovu
rm -rf ${OUTDIR}
mkdir -p ${OUTDIR}

# Export all instrument series as Parquet.
# pond copy on table:series entries exports Parquet via DataFusion.
# Directory structure is preserved: /hydrovu/devices/<id>/<Name>.series → hydrovu/devices/<id>/<Name>.series
${EXE} copy '/hydrovu/devices/**/*.series' host://${OUTDIR}

echo ""
echo "=== Verification ==="
find ${OUTDIR} -name '*.series' -exec ls -lh {} \;

echo ""
echo "Archive complete. Next steps:"
echo "  1. git add noyo/hydrovu/"
echo "  2. Update noyo/hydrovu.yaml (mark inactive instruments)"
echo "  3. Update noyo/setup.sh (import archived Parquet files)"
echo "  4. Rebuild pond and verify /combined/* still works"
