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

## Part D: RabbitMQ stream

Install dependencies and start a RabbitMQ broker. The scripts use a local broker by default; for a hosted
broker, set `RABBITMQ_URL` without committing it to Git.

```bash
pip install -r requirements.txt
python stream/driver.py Ravi 'bangalore.koramangala.*'
python stream/driver.py Meena 'bangalore.*.food'
python stream/driver.py Control 'bangalore.#'
python stream/dispatcher.py
```

The dispatcher sends a request every two seconds. Every tenth request deliberately omits `zone`; drivers
reject it with `basic_nack(requeue=False)` and acknowledge valid requests with `basic_ack`.
