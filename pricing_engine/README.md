# Internal Dynamic Pricing Engine

Python 3.12 backend service for internal-data–powered dynamic pricing with:

- Daily feature materialization from internal MySQL tables
- Per-SKU elasticity via StatsModels (log-log OLS)
- Global demand model via LightGBM
- Price Optimization Engine (POE) with vendor rules & constraints
- Flask REST API for price suggestions & feedback
- Monitoring + feedback loop for continuous improvement

---

## 1. Setup

### 1.1 Prereqs

- Python 3.12
- MySQL 8.x (or compatible)
- `virtualenv` or `pyenv` recommended

### 1.2 Install

```bash
git clone <this-repo> pricing_engine
cd pricing_engine

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install --upgrade pip
pip install -r requirements.txt
