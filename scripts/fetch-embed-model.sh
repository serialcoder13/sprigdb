#!/usr/bin/env bash
# Downloads the embedding model the docs pack and docs search use,
# BAAI/bge-small-en-v1.5 (MIT licence), from Hugging Face into
# target/models/bge-small-en-v1.5/ (gitignored), and checks each file's SHA-256.
#
# Only the three files the embedder reads are fetched, from a pinned revision:
# config.json, tokenizer.json and model.safetensors (about 133 MB).
#
# Idempotent: files already present with the right hash are kept. Prints the
# model directory on the last line, so
#
#   SPRIG_EMBED_MODEL_DIR=$(scripts/fetch-embed-model.sh | tail -1)
#
# sets up the model tests and the docs-pack tool.
set -euo pipefail

REPO="BAAI/bge-small-en-v1.5"
REVISION="5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"
FILES=(
  "config.json 094f8e891b932f2000c92cfc663bac4c62069f5d8af5b5278c4306aef3084750"
  "tokenizer.json d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66"
  "model.safetensors 3c9f31665447c8911517620762200d2245a2518d6e7208acc78cd9db317e21ad"
)

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dir="${SPRIG_EMBED_MODEL_DIR:-$root/target/models/bge-small-en-v1.5}"
mkdir -p "$dir"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

for entry in "${FILES[@]}"; do
  name="${entry%% *}"
  want="${entry##* }"
  path="$dir/$name"
  if [[ -f "$path" && "$(sha256 "$path")" == "$want" ]]; then
    echo "ok       $name" >&2
    continue
  fi
  echo "fetching $name" >&2
  tmp="$path.part"
  curl --fail --location --silent --show-error --retry 3 \
    -o "$tmp" "https://huggingface.co/$REPO/resolve/$REVISION/$name"
  got="$(sha256 "$tmp")"
  if [[ "$got" != "$want" ]]; then
    rm -f "$tmp"
    echo "error: $name has SHA-256 $got, expected $want" >&2
    exit 1
  fi
  mv "$tmp" "$path"
  echo "ok       $name" >&2
done

echo "$dir"
