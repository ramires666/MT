# Refactor Structure Plan

Updated: 2026-04-02

## Goal

Reduce structural complexity and improve testability without breaking the current research workflow.

Primary aims:
- split oversized modules into feature-oriented units
- reduce Bokeh UI coupling to domain/storage/tools
- unify duplicated scan/scanner persistence and orchestration patterns
- isolate configuration, state, and job orchestration logic
- keep current UX behavior stable while refactoring internals

Documentation basis for this plan:
- `src/` currently contains `927` indexed Python definitions across `70` files
- `src/bokeh_app/main.py` alone currently contains `320` indexed definitions
- the detailed narrative reference is `docs/reference/system_functional_reference.md`
- the exact generated definition appendix is `docs/reference/src_callable_index.md`

## 1. Current Structural Problems

## 1.1 `src/bokeh_app/main.py` Is A Monolith

Symptoms:
- about `8.2k` lines
- about `320` indexed nested/module-level definitions
- mixes view construction, state, orchestration, persistence restore, result adaptation, polling, and UI callbacks

Main issues:
- high regression surface
- low unit-testability because many behaviors are nested closures
- cross-feature edits land in one file
- import fan-in from UI, domain, storage, and tools is too high

## 1.2 UI Depends Directly On Domain And Storage

Current pattern:
- `main.py` imports domain runners and storage loaders directly
- UI callbacks also perform persistence operations

Impact:
- no clean application-service layer
- business rules are distributed across callbacks
- hard to reuse workflows outside Bokeh

## 1.3 Scan / Scanner Orchestration Is Similar But Not Unified

Current duplication:
- universe resolution
- allowed pair filtering
- progress reporting
- cancellation
- partial results
- resume from snapshots

Files involved:
- `src/domain/scan/johansen_universe.py`
- `src/domain/scan/optimizer_grid_scan.py`
- `src/storage/scan_results.py`
- `src/storage/scanner_results.py`

## 1.4 Snapshot Persistence Is Repeated Across Features

Separate implementations exist for:
- Johansen scan snapshots
- optimizer scanner snapshots
- WFA snapshots

Common repeated concerns:
- request signature construction
- strict compatibility filtering
- partial vs complete lifecycle
- latest snapshot lookup
- conversion from saved frame back into runtime result objects

## 1.5 Meta Selector And Portfolio Are Functionally Broad

Meta Selector:
- orchestration in `meta_selector.py`
- ML fitting/ranking in `meta_selector_ml.py`
- output shaping in `meta_selector_outputs.py`
- types in `meta_selector_types.py`

Portfolio:
- item window logic
- allocation derivation
- risk series building
- correlation analysis
- combined curve composition
- final summary metrics

In both cases the current module split is workable, but still too broad and not always obvious.

## 1.6 Synthetic Group Logic Lives In A Low-Level Filter Module

Current reality:
- `domain.data.catalog_groups` now also knows how to read the special CSV pair list and derive exact partners/pair keys

Why this is a smell:
- pure catalog filtering and synthetic-universe sourcing are different responsibilities
- the module now reaches toward storage-oriented concerns

## 2. Target Architecture

Recommended high-level layering:

1. UI layer
- pure Bokeh widgets, layouts, and source patching

2. Application/orchestration layer
- request assembly
- progress/cancel policy
- snapshot restore/persist orchestration
- replay contracts

3. Domain layer
- backtest / optimizer / scans / WFA / meta / portfolio logic

4. Storage layer
- persistence paths
- snapshot schemas
- load/save/restore helpers

5. Tools/integration layer
- MT5 terminal export
- batch sync
- external import/export flows

## 3. Proposed Refactor Steps

## Stage 1: Split `bokeh_app/main.py` By Feature

Target modules:
- `bokeh_app/controllers/tester.py`
- `bokeh_app/controllers/optimizer.py`
- `bokeh_app/controllers/scan.py`
- `bokeh_app/controllers/scanner.py`
- `bokeh_app/controllers/wfa.py`
- `bokeh_app/controllers/meta.py`
- `bokeh_app/controllers/portfolio.py`
- `bokeh_app/controllers/downloader.py`
- `bokeh_app/layout.py`
- `bokeh_app/runtime_state.py`

What moves first:
- pure helper families
- export metadata builders
- progress/render helpers
- run/poll/cancel handlers

Expected gain:
- smaller files
- clearer ownership
- easier tests for each block

## Stage 2: Introduce Application Services

Create feature services between UI and domain/storage:
- `app_services/tester_service.py`
- `app_services/scan_service.py`
- `app_services/scanner_service.py`
- `app_services/wfa_service.py`
- `app_services/meta_service.py`
- `app_services/portfolio_service.py`
- `app_services/download_service.py`

Responsibilities:
- build normalized requests
- own restore/resume decision logic
- own signature assembly
- own persistence timing rules

Expected gain:
- Bokeh callbacks become thinner
- same flows become reusable outside UI

## Stage 3: Unify Universe And Pair Filtering

Extract:
- exact pair list resolution
- saved scan partner filtering
- special synthetic group logic

Possible target:
- `domain/pairs/universe_resolver.py`
- `domain/pairs/pair_filters.py`

Expected gain:
- one place for:
  - group -> symbol universe
  - group -> exact allowed pairs
  - symbol -> exact partners

## Stage 4: Unify Snapshot Stores

Create a shared snapshot toolkit:
- frame schema helpers
- signature normalization
- latest snapshot selection
- partial/complete lifecycle helpers

Possible target:
- `storage/snapshots/base.py`
- `storage/snapshots/signatures.py`
- `storage/snapshots/frame_store.py`

Expected gain:
- less duplicated persistence logic
- fewer edge-case fixes repeated across features

## Stage 5: Split Portfolio Module

Recommended modules:
- `domain/portfolio_items.py`
- `domain/portfolio_allocation.py`
- `domain/portfolio_analysis.py`
- `domain/portfolio_curves.py`
- `domain/portfolio_metrics.py`

Expected gain:
- cleaner mental model
- targeted tests for each responsibility

## Stage 6: Split Meta Selector By Responsibility

Recommended modules:
- `domain/meta/features.py`
- `domain/meta/datasets.py`
- `domain/meta/models.py`
- `domain/meta/pipeline.py`
- `domain/meta/outputs.py`

Expected gain:
- cleaner boundary between feature engineering and model execution
- no duplicated config normalization knowledge between UI and ML internals

## Stage 7: Normalize Config Ownership

Current smell:
- model config normalization exists in both UI and domain code
- some worker/path settings are interpreted in multiple places

Refactor:
- move normalization to domain/service layer only
- UI should build raw user input payloads, not authoritative normalized configs

## 4. Testing Plan For The Refactor

Keep three test layers:

1. Pure unit tests
- search-space parsing
- pair filter logic
- snapshot signature logic
- portfolio metrics
- meta feature engineering

2. Service tests
- request -> runner -> persisted snapshot decisions
- resume and cancel behavior
- exact pair filtering behavior

3. UI smoke tests
- build Bokeh document
- verify block visibility/toggle state
- verify table replay hooks still wire correctly

Highest-value first additions:
- thin tests for extracted app services
- tests for exact-pair group rules independent of UI
- tests for snapshot compatibility checks independent of Bokeh

## 5. Migration Order

Recommended low-risk order:

1. Extract pure helpers from `main.py`
2. Extract service-layer request builders and restore/resume logic
3. Move scan/scanner signature + pair-filter logic into shared modules
4. Unify snapshot helpers
5. Split portfolio and meta-selector internals
6. Only after that consider broader UI layout decomposition

Why this order:
- it preserves current runtime behavior
- it reduces the size of `main.py` early
- it avoids changing domain math and UI behavior at the same time

## 6. Concrete Structural Targets

### `main.py`
- target: orchestration root under `2k` lines
- should mostly contain:
  - document bootstrap
  - widget composition
  - wiring of imported controllers

### Scan / Scanner
- target: shared pair-scan infrastructure with strategy-specific execution hooks

### Storage
- target: one snapshot vocabulary:
  - signature
  - scope
  - partial
  - complete
  - restore
  - latest

### Meta / Portfolio
- target: each file owns one primary responsibility

## 7. Known Risks During Refactor

- UI replay behavior is easy to regress because many blocks share tester state
- signature-based caches can silently fork if normalization changes
- Bokeh nested closures currently capture a lot of ambient state; extraction must preserve ordering and lifecycle
- portfolio and scanner restore behavior are sensitive to current snapshot semantics

## 8. Success Criteria

The refactor is successful when:
- `main.py` is materially smaller
- new service modules can be unit-tested without building a full Bokeh document
- scan/scanner persistence bugs are fixed in shared code once, not in multiple stores
- exact pair filtering and saved-scan partner logic live in one authoritative place
- meta-selector and portfolio modules become easier to read without changing outputs
