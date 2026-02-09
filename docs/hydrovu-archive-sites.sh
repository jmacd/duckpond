#!/bin/sh
# ARCHIVE HYDROVU INSTRUMENT DATA
#
# This script archives active instrument series in the pond and exports
# the archived Parquet files for git storage.
#
# How it works:
#   1. `pond run /etc/hydrovu archive` renames _active.series to
#      _archive_YYYYMMDD.series inside the pond (atomic rename)
#   2. `pond copy --strip-prefix=/hydrovu` exports archive Parquet files
#      to noyo/hydrovu/ without doubling the hydrovu/ path prefix
#   3. You git-add the exported Parquet files
#   4. Mark the devices as active: false in hydrovu.yaml
#   5. Update setup.sh to import the archived Parquet at pond init
#
# The factory layer (combine.yaml) uses _* globs so it sees both active
# and archive files — no changes needed there.
#
# To archive a single device:
#   pond run /etc/hydrovu archive --device NoyoCenterVulink_1
#
# Instrument inventory:
#   NoyoCenterVulink_1  (4990594334457856)  FieldStation surface (was BDock)
#   NoyoCenterAT500_1   (6154708427603968)  FieldStation surface (was BDock)
#   NoyoCenterVulink_4  (5877040213786624)  FieldStation bottom  (was Princess)
#   NoyoCenterAT500_3   (6187749627789312)  FieldStation bottom  (was Princess)
#   NoyoCenterVulink_2  (6582334615060480)  INACTIVE — Silver, failed May 2024
#   NoyoCenterVulink_3  (6417476404772864)  INACTIVE — Silver, decommissioned
#   NoyoCenterAT500_2   (5859376846209024)  INACTIVE — Silver, decommissioned

set -x
set -e

ROOT=/Volumes/sourcecode/src/duckpond
NOYO=${ROOT}/noyo
POND=${NOYO}/pond
EXE=${ROOT}/target/debug/pond

export POND

OUTDIR=${NOYO}/hydrovu
mkdir -p ${OUTDIR}

# Step 1: Archive — renames _active.series → _archive_YYYYMMDD.series in pond
${EXE} run /etc/hydrovu archive

# Step 2: Export archive files as Parquet to host filesystem
# --strip-prefix=/hydrovu removes the leading /hydrovu/ from pond paths,
# so /hydrovu/devices/123/foo.series → ${OUTDIR}/devices/123/foo.series
${EXE} copy '/hydrovu/devices/**/*_archive_*.series' host://${OUTDIR} --strip-prefix=/hydrovu

echo ""
echo "=== Exported archive files ==="
find ${OUTDIR} -name '*.series' -exec ls -lh {} \;
echo ""
echo "Next steps:"
echo "  1. git add noyo/hydrovu/"
echo "  2. Mark archived devices as active: false in noyo/hydrovu.yaml"
echo "  3. Add 'pond copy host://...hydrovu /hydrovu' to noyo/setup.sh"
