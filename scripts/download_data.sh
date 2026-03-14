#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Download DLD open datasets (public, no auth required)
# Run from the project root:  bash scripts/download_data.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DATA_DIR="data"
mkdir -p "$DATA_DIR"

echo "⬇️  Downloading DLD open datasets into ./$DATA_DIR/ ..."

# 1. Transactions (~971 MB)
echo "  [1/5] transactions.csv ..."
curl -L -o "$DATA_DIR/transactions.csv" \
  "https://www.dubaipulse.gov.ae/dataset/3b25a6f5-9077-49d7-8a1e-bc6d5dea88fd/resource/a37511b0-ea36-485d-bccd-2d6cb24507e7/download/transactions.csv"

# 2. Projects
echo "  [2/5] projects.csv ..."
curl -L -o "$DATA_DIR/projects.csv" \
  "https://www.dubaipulse.gov.ae/dataset/0b782e64-5950-4507-8f6e-02a0c30c7054/resource/db35b0cd-d291-4dde-b176-9b8d5765c7d9/download/projects.csv"

# 3. Units (freehold)
echo "  [3/5] units.csv ..."
curl -L -o "$DATA_DIR/units.csv" \
  "https://www.dubaipulse.gov.ae/dataset/85462a5b-08dc-4325-9242-676a0de4afc4/resource/7d4deadf-c9bc-47a4-85de-998d0ce38bf3/download/units.csv"

# 4. Developers
echo "  [4/5] developers.csv ..."
curl -L -o "$DATA_DIR/developers.csv" \
  "https://www.dubaipulse.gov.ae/dataset/ac68c7d5-8acb-441c-9a7d-6e6d72942d86/resource/57ca3b1a-775d-4f6c-8b04-19e02f6b4a03/download/developers.csv"

# 5. Residential sale price index
echo "  [5/5] residential_sale_index.csv ..."
curl -L -o "$DATA_DIR/residential_sale_index.csv" \
  "https://www.dubaipulse.gov.ae/dataset/342a48fc-6499-40b4-9323-b9c0a536f57f/resource/ec4bef3f-d995-4487-9ec3-f7bd2d3788eb/download/residential_sale_index.csv"

echo ""
echo "✅ All files downloaded:"
ls -lh "$DATA_DIR"/*.csv
echo ""
echo "Next step:  python -m backend.ingestion.csv_loader"
