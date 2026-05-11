# Docs Reorganization Design

**Date:** 2026-05-11

## Goal

Replace the single `docs/API.md` with a structured set of documentation:
1. API catalog/index (`api_index.md`) — overview of all APIs by module
2. Per-module detailed API docs (5 files: `api_types.md`, `api_schema.md`, `api_storage.md`, `api_registry.md`, `api_client.md`)
3. Project overview (`overview.md`)

## Target Structure

```
docs/
├── overview.md
├── api_index.md
├── api_types.md
├── api_schema.md
├── api_storage.md
├── api_registry.md
├── api_client.md
├── CI_CD.md              (untouched)
├── examples/             (untouched)
```

`docs/API.md` is deleted. `README.md` is updated to link to the new files.

## Document Specifications

### 1. `api_index.md` — API Catalog

Table-driven index. Content:
- Module overview table: module name, source file, detail doc, description
- API quick index table: API name, module, detail doc, one-line description
- Exception index table: exception class, module, description

### 2. Per-module detail docs (5 files)

Template for each:
- Module overview paragraph
- Public exports table (symbol, type, description)
- Detailed API section per symbol: signature, parameter table, return, code examples
- Cross-references to related modules

### 3. `overview.md` — Project Introduction

Content:
- Purpose — what problem the feature store solves
- Project structure — source tree with descriptions
- Capabilities — feature list
- How it works — data flow: registration → write → read → model assembly → training export
- Quick start — minimal example
- Installation — pip options

### README.md update

Update the documentation links table to reflect new file structure.

## Constraints

- English, markdown
- Replace API.md, don't keep it
- All 5 source modules get their own detail doc
- Content sourced from existing API.md + source code — no new information needed
