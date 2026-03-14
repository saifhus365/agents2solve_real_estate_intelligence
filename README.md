# Dubai Real Estate Intelligence Co-Pilot

A production-grade **GraphRAG co-pilot** for Dubai real estate professionals. Ask natural-language questions about the Dubai property market and get grounded, cited answers powered by:

- **Neo4j** knowledge graph traversal
- **BAAI/bge-large-en-v1.5** vector search via Pinecone
- **PostgreSQL** timeseries aggregations
- **LangGraph** multi-agent pipeline with grounding validation
- **NVIDIA Llama-3.1-Nemotron-70B-Instruct** for answer generation

## Architecture

```
User Query
    │
    ▼
┌──────────────────────┐
│  Query Classifier     │  (graph | vector | hybrid | timeseries)
└──────────┬───────────┘
           │
    ┌──────┼──────────────────┐
    ▼      ▼                  ▼
┌──────┐ ┌──────────┐ ┌──────────┐
│Cypher│ │ Vector   │ │   SQL    │
│ Gen  │ │ Search   │ │  Query   │
└──┬───┘ └────┬─────┘ └────┬─────┘
   ▼          │             │
┌──────┐      │             │
│Neo4j │      │             │
│Query │      │             │
└──┬───┘      │             │
   └──────────┼─────────────┘
              ▼
       ┌──────────────┐
       │   Reranker    │  (cross-encoder top-8)
       └──────┬───────┘
              ▼
       ┌──────────────┐
       │  Grounding    │  (hallucination filter)
       └──────┬───────┘
              ▼
       ┌──────────────┐
       │  Generator    │  (NVIDIA API + SSE streaming)
       └──────────────┘
```

## Prerequisites

- Python 3.11+
- Node.js 20+
- Docker & Docker Compose
- External services with credentials:
  - **NVIDIA API** key
  - **Neo4j AuraDB** instance
  - **PostgreSQL** (Supabase or similar)
  - **Pinecone** serverless account
  - **Redis** (included in Docker Compose)
  - **W&B** account (for eval tracking)

## Quick Start

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd agents2solve_real_estate_intelligence
cp .env.example .env
# Edit .env with your credentials (NVIDIA, Neo4j, PostgreSQL, Pinecone)
```

### 2. Download and load DLD data (no API keys needed!)

All DLD datasets are publicly downloadable. This is the fastest path to a working demo:

```bash
# Download all 5 CSV datasets (~1 GB total)
bash scripts/download_data.sh

# Load into PostgreSQL + Neo4j (full load)
python -m backend.ingestion.csv_loader

# Or load a sample for quick demo (first 5000 rows per file)
python -m backend.ingestion.csv_loader --limit 5000
```

This loads **transactions, projects, and developers** directly into PostgreSQL and builds the full Neo4j knowledge graph — no DLD API credentials required.

> **Note**: DLD API credentials (`DLD_CLIENT_ID` / `DLD_CLIENT_SECRET`) are only needed later for the live daily Celery sync job. Request access at [data.dubai](https://data.dubai) when ready.

### 3. Run with Docker Compose

```bash
docker compose up --build
```

This starts:
| Service | Port | Description |
|---------|------|-------------|
| Backend | 8000 | FastAPI + SSE streaming |
| Frontend | 5173 | React + Vite dev server |
| Redis | 6379 | Celery broker |
| Celery Worker | — | DLD sync & document ingestion |
| Celery Beat | — | Daily scheduler (02:00 UAE) |

### 4. Run manually (development)

**Backend:**
```bash
pip install -r backend/requirements.txt
uvicorn backend.main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

**Celery worker + beat (optional — for daily live sync):**
```bash
celery -A backend.ingestion.tasks worker --loglevel=info -Q ingestion
celery -A backend.ingestion.tasks beat --loglevel=info
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/chat` | SSE streaming chat (query + session_id) |
| GET | `/api/health` | Service health check |
| POST | `/api/eval` | Trigger evaluation run |

### Chat SSE Events

```
data: {"type": "token", "content": "The average..."}
data: {"type": "citations", "data": [{...}]}
data: {"type": "done", "latency_ms": 1234}
```

## Data Pipeline

### Option A: CSV Load (recommended for demo)

```bash
bash scripts/download_data.sh                     # download ~1 GB of data
python -m backend.ingestion.csv_loader             # full load
python -m backend.ingestion.csv_loader --limit 5000  # quick sample
```

### Option B: Live API Sync (requires DLD credentials)

1. **DLD Sync** — Daily at 02:00 UAE: fetches transactions via Dubai Pulse API → PostgreSQL → Neo4j area stats
2. **Document Loader** — Weekly: ingests RERA PDFs + news RSS → chunks → BAAI embeddings → Pinecone
3. **Graph Builder** — On-demand: reads PostgreSQL → builds full Neo4j graph

## Evaluation

Run the evaluation harness against 50 gold Q&A pairs:

```bash
# Via API
curl -X POST http://localhost:8000/api/eval \
  -H "Content-Type: application/json" \
  -d '{"run_name": "eval-v1", "ablation": true}'
```

Metrics logged to W&B:
- **answer_faithfulness** — claims traceable to context
- **retrieval_recall@10** — relevant chunks in top 10
- **cypher_accuracy** — correct Neo4j entity retrieval
- **multihop_accuracy** — graph vs vector vs hybrid comparison
- **hallucination_rate** — ungrounded claims
- **p95_latency_ms** — 95th percentile end-to-end latency

## Tech Stack

| Layer | Technology |
|-------|-----------|
| LLM | NVIDIA API (Llama-3.1-Nemotron-70B) |
| Agent | LangGraph state machine |
| Embeddings | BAAI/bge-large-en-v1.5 (1024d) |
| Vector Store | Pinecone serverless |
| Graph DB | Neo4j AuraDB |
| Relational DB | PostgreSQL (asyncpg) |
| Backend | FastAPI (async + SSE) |
| Frontend | React + Vite + Tailwind + MapLibre GL |
| Tasks | Celery + Redis |
| Eval | W&B |

## License

MIT