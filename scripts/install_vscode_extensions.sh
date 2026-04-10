#!/usr/bin/env bash
set -euo pipefail

# Install recommended VS Code extensions for viewing spreadsheets and PDFs
# Usage: ./scripts/install_vscode_extensions.sh

RECOMMENDED=(
  "RandomFractalsInc.data-preview"  # Data Preview (preview .xlsx/.csv)
  "janisdd.vscode-edit-csv"         # CSV editor
  "mechatroner.rainbow-csv"         # CSV highlighting
  "tomoki1207.pdf"                 # PDF preview (optional)
)

if ! command -v code >/dev/null 2>&1; then
  echo "Error: 'code' CLI not found. Install or add VS Code CLI to PATH." >&2
  echo "On Windows use 'Open with Code' or enable 'code' command from VS Code Command Palette." >&2
  exit 2
fi

for ext in "${RECOMMENDED[@]}"; do
  echo "Installing extension: $ext"
  code --install-extension "$ext" --force || true
done

echo "Done. Restart VS Code if it was open."
