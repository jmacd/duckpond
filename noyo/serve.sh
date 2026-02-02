#!/bin/bash
# Start the dev server
cd "$(dirname "$0")"
npx vite export --port 5173
