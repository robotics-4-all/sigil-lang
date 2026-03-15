# designer -- Visual Pipeline Designer

## OVERVIEW

React/TypeScript/Vite visual designer for building sigil pipelines via drag-and-drop canvas. Generates `.sigil` DSL text from a structured model. Standalone app — runs independently from the sigil Python package.

## STACK

React + TypeScript + Vite, React Flow (canvas/nodes/edges), Zustand (state), nanoid (IDs).

## STRUCTURE

```
designer/
+-- src/
|   +-- main.tsx                     # Entry (110 lines): ReactFlow provider, StoreContext
|   +-- SigilDesigner.tsx            # Root component (63 lines)
|   +-- index.ts                     # Package export (3 lines)
|   +-- store/
|   |   +-- useDesignerStore.ts      # Zustand store (200 lines): full CRUD for model
|   +-- types/
|   |   +-- model.ts                 # TypeScript model types (218 lines)
|   +-- codegen/
|   |   +-- generateSigil.ts         # Model -> .sigil text (233 lines)
|   +-- utils/
|   |   +-- stageDefaults.ts         # Default field values per StageKind (17 lines)
|   +-- components/
|       +-- Canvas/
|       |   +-- PipelineCanvas.tsx   # ReactFlow canvas, node rendering, edge logic (407 lines)
|       +-- nodes/                   # 13 stage nodes + BaseNode
|       |   +-- BaseNode.tsx         # Shared node wrapper (58 lines)
|       |   +-- *Node.tsx            # One per stage kind, ~21 lines each
|       +-- Inspector/
|       |   +-- Inspector.tsx        # Right-panel inspector (324 lines)
|       |   +-- *Form.tsx            # 16 forms, one per stage/output type
|       +-- Sidebar/
|           +-- LeftSidebar.tsx      # Stage palette (153 lines)
+-- vite.config.ts
+-- package.json
+-- Dockerfile
+-- run.sh
```

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| Model type definitions | `src/types/model.ts` | `SigilModel`, `PipelineDef`, `StageKind`, all stage interfaces |
| State management | `src/store/useDesignerStore.ts` | `createDesignerStore(initialModel?)`, used via React context |
| DSL text generation | `src/codegen/generateSigil.ts` | `generateSigil(model: SigilModel): string` |
| Canvas / node layout | `src/components/Canvas/PipelineCanvas.tsx` | ReactFlow setup, node/edge wiring |
| Add a new stage node | `src/components/nodes/` | Copy any `*Node.tsx` pattern; add to `stageDefaults.ts` |
| Add inspector form for stage | `src/components/Inspector/` | Add `*Form.tsx`; wire into `Inspector.tsx` |
| Add stage to codegen | `src/codegen/generateSigil.ts` | Add `case` to switch in `generateSigil()` |
| Stage default values | `src/utils/stageDefaults.ts` | Keyed by `StageKind` |

## KEY TYPES (model.ts)

```typescript
SigilModel    = { brokers, constants, functions, schemas, pipelines }
PipelineDef   = { id, name, source, join?, letBindings, stages, sink?, route?, tee?, fanout?, onError?, retry? }
StageKind     = 'filter' | 'map' | 'window' | 'aggregate' | 'having' | 'deduplicate'
              | 'rate' | 'flatten' | 'distinct' | 'throttle' | 'sample' | 'group_by' | 'accumulate'
              // NOTE: 'predict' NOT yet in StageKind — pending addition
```

All stage interfaces use a discriminant `kind` field. IDs always via `nanoid()`.

## CONVENTIONS

- **Node pattern**: Every stage node imports `BaseNode` and exports a thin wrapper that passes a label. `*Node.tsx` files are ~21 lines each. Never put logic in nodes.
- **Store access**: Only via `useDesignerStore` hook (React context). No prop-drilling for model state.
- **ID generation**: Always `nanoid()` for all model entity IDs (brokers, pipelines, stages, etc.).
- **Codegen switch**: `generateSigil.ts` uses `switch(s.kind)` to emit DSL per stage. Uses `formatDuration(d: DurationDef)` helper for duration values.
- **No tests**: `designer/` has no test suite. No Jest/Vitest config.
- **One-off scripts**: `update_nodes.py`, `update_nodes_bg.py` are migration scripts — not part of the app.

## GAPS (not yet implemented in designer)

- `predict` stage: not in `StageKind`, no node, no form, no codegen case
- Schema declarations: model type exists but no UI
- `on_error` / `retry`: type fields exist in `PipelineDef` but no form UI
- const/func definitions: `SigilModel` has fields but no inspector forms

## COMMANDS

```bash
# Dev server
cd designer && npm install && npm run dev

# Build
npm run build

# Docker
docker build -t sigil-designer designer/
docker run -p 3000:3000 sigil-designer
# or via run.sh
```
