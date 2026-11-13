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

## Bonus: Stream into the lakehouse

After running the FreshCart ELT notebook in the same Colab runtime, run
`stream/bonus_stream_lakehouse_colab.ipynb`. It publishes two web-schema FreshCart order events,
collects them from RabbitMQ for one minute, writes them with `extract.write_landing()`, loads the raw
file into `bronze.orders_raw`, and reruns the transformation SQL through `gold.fct_orders`.

The notebook then replays that exact landing file. A successful replay reports zero newly loaded bronze
rows and unchanged gold totals.
