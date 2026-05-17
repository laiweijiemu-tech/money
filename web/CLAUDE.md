# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A lightweight stock monitoring system (短线打板盯盘系统) for short-term traders. Focuses on market themes, leading stocks, and real-time alerts. Frontend + Backend monorepo.

## Architecture

- **Frontend** (`frontend/`): Vue 3 + Vite + ECharts. Single-page app that proxies `/api` requests to the backend.
- **Backend** (`backend/`): FastAPI (Python). Provides market data APIs. Uses SQLite (`backend/data/watchtower.db`) for storage.
- Communication: Frontend polls backend REST endpoints; backend fetches data from free market data sources (东方财富, 腾讯财经, AkShare).

## Commands

### Frontend
```bash
cd frontend
npm install          # install dependencies
npm run dev          # dev server (proxies /api to backend)
npm run build        # type-check (vue-tsc) then vite build
```

### Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload   # dev server on :8000
```

### Proxy Configuration
Set `VITE_API_PROXY_TARGET` env var to change the backend URL (defaults to `http://127.0.0.1:8000`).

## Key Backend Structure

- `backend/app/main.py` — FastAPI app, routes, startup
- `backend/app/database.py` — SQLite init/access
- `backend/app/services/live_data.py` — Real market data fetching
- `backend/app/services/mock_data.py` — Mock data for development

## Language

The project uses Chinese for UI text, comments, and documentation. Code identifiers are in English.
