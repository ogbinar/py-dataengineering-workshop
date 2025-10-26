# ─────────────────────────────────────────────────────────────
# paths.py — Centralized Directory Configuration
# ─────────────────────────────────────────────────────────────
# Defines the standardized folder structure for the entire
# data engineering workflow. Each stage corresponds to a
# clear processing phase (raw → clean → model → sandbox).
# This ensures consistency, portability, and reproducibility
# across all scripts and environments.
# ─────────────────────────────────────────────────────────────

from pathlib import Path

# Base data directory (root for all project data assets)
DATA = Path("data")

# Layer 1: Raw data — direct outputs from extraction (no modifications)
RAW = DATA / "00-raw"

# Layer 2: Clean data — standardized, validated, and type-corrected tables
CLEAN = DATA / "01-clean"

# Layer 3: Model data — transformed datasets ready for analytics, ML, or BI
MODEL = DATA / "02-model"

# Layer 4: Sandbox — ad-hoc experiments, temporary exports, and quick tests
SANDBOX = DATA / "03-sandbox"

# ─────────────────────────────────────────────────────────────
# Ensure all necessary directories exist before writing.
# This makes the pipeline resilient to missing folders
# when running on fresh environments or cloned repos.
# ─────────────────────────────────────────────────────────────
CLEAN.mkdir(parents=True, exist_ok=True)         # create /01-clean
MODEL.mkdir(parents=True, exist_ok=True)         # create /02-model
(CLEAN / "_dq").mkdir(parents=True, exist_ok=True)  # subfolder for DQ logs & reports
