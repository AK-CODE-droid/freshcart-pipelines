# freshcart-pipelines

University Assignment: **From Git to Gold — Version Control, ELT, ML Pipelines and Event Streaming**

## Prescribed Directory Structure
```
freshcart-pipelines/
├── README.md
├── .gitignore              <- ignores .env, *.pkl, *.csv, mage.log, __pycache__/
├── requirements.txt
├── elt/                    <- FreshCart ELT pipeline on Snowflake (Part B)
├── iris_project/           <- Mage ML pipeline project (Part C)
├── stream/                 <- RabbitMQ dispatcher and driver scripts (Part D)
├── tests/
│   └── test_prepare_data.py
└── .github/workflows/ci.yml
```

## Setup & Local Testing
1. Create and activate a Python 3.12 virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run unit tests:
   ```bash
   pytest
   ```
