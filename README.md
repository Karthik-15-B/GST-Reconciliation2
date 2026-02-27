# GST Reconciliation – Foundation

Minimal full-stack foundation proving connectivity between **FastAPI**, **Streamlit**, **MongoDB Atlas**, and **Neo4j Aura**.

## Project Structure

```
project-root/
├── backend/
│   ├── main.py            # FastAPI entry point
│   ├── database.py        # MongoDB + Neo4j connection managers
│   ├── models.py          # Pydantic models
│   └── routes/
│       ├── health.py      # GET /health
│       ├── test_mongo.py  # POST & GET /test/mongo
│       └── test_neo4j.py  # POST & GET /test/neo4j
├── frontend/
│   ├── dashboard.py       # Streamlit app
│   └── api_client.py      # HTTP helpers for calling backend
├── .env                   # Credentials (not committed)
├── .gitignore
├── requirements.txt
└── README.md
```

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy .env.example to .env and fill in your credentials
#    (or ensure .env already has the correct values)
```

## Running

### Backend (FastAPI)

```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

API docs available at: `http://localhost:8000/docs`

### Frontend (Streamlit)

```bash
streamlit run frontend/dashboard.py --server.port 8501
```

Open: `http://localhost:8501`

## Verification

1. Start the backend.
2. Start the frontend.
3. Navigate to **System Health** → click **Check Health** → both should show **UP**.
4. Navigate to **Database Tests** → test MongoDB and Neo4j insert/fetch.
