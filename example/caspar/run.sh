#!/bin/bash -e

export RUST_BACKTRACE=1

DIR=/Users/jmacd/src/duckpond/example/caspar
INBOX=${DIR}/inbox
POND=${DIR}/.caspar.pond
EXE=${DIR}/../..//target/debug/duckpond

cargo build

rm -rf ${INBOX}
mkdir ${INBOX}

export POND

rm -rf ${POND}
echo ---- init
${EXE} init || exit 1

echo ---- apply inbox
${EXE} apply -f ${DIR}/inbox.yaml || exit 1
cp /Volumes/sourcecode/src/caspar.water/data.csv ${INBOX}

echo ---- apply import
${EXE} apply -f ${DIR}/import.yaml || exit 1

echo ---- apply reduce
${EXE} apply -f ${DIR}/reduce.yaml || exit 1

echo ---- run
${EXE} run || exit 1
