# FreshCart Data Pipelines

A Python-based data engineering project that brings together **ELT, data warehousing, machine-learning workflows, and event streaming**. The repository contains three practical pipelines:

1. **FreshCart ELT** — extracts order, CRM, foreign-exchange, and pincode data; loads raw records into Snowflake; transforms them into silver and gold models.
2. **Iris ML pipeline** — uses Mage blocks to load, validate, prepare, train, evaluate, and export an Iris classifier.
3. **RabbitMQ streaming** — demonstrates topic-based message dispatch and consumption, plus a FreshCart event path that can land streamed orders in the Snowflake lakehouse.

> This repository is an educational/portfolio project. External API endpoints and Snowflake credentials are supplied by the operator; mock mode is available for the ELT extraction flow.

## Contents

- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Repository structure](#repository-structure)
- [Getting started](#getting-started)
- [FreshCart ELT pipeline](#1-freshcart-elt-pipeline)
- [Iris machine-learning pipeline](#2-iris-machine-learning-pipeline)
- [RabbitMQ streaming](#3-rabbitmq-streaming)
- [Tests and continuous integration](#tests-and-continuous-integration)
- [Configuration and security](#configuration-and-security)

## Architecture

### FreshCart ELT

```text
Web Orders ─┐
Mobile Orders ─┤
CRM Events ────┼──> Extract APIs / mock data
FX Rates ──────┤           |
Pincode API ───┘           v
                    Gzipped JSONL landing files
                             |
                             v
                    Snowflake internal stage
                             |
                             v
                       BRONZE tables
                             |
                             v
                 SILVER cleansing + deduplication
                             |
               Customer SCD Type 2 / FX / geography
                             |
                             v
                    GOLD fact_orders
                             |
                             v
                     Quality checks
```

The extract step preserves source records in gzipped newline-delimited JSON files with source, run ID, and extraction timestamp metadata. Snowflake handles the transformations.

### Iris ML workflow

```text
Load Iris data
     |
Validate input
     |
Prepare train/test split (80/20)
     |
Train classifier
    / \
Evaluate  Save results
```

The Mage pipeline is defined in `iris_project/pipelines/iris_classifier/metadata.yaml`.

### Event streaming

The RabbitMQ demonstration uses a durable topic exchange. Drivers create private queues and bind routing patterns such as `bangalore.*.food`. The FreshCart streaming integration consumes order events, writes a landing file, loads bronze, and reruns the warehouse transformations.

## Technology stack

- **Language:** Python 3.12
- **Data processing / ML:** pandas, scikit-learn, joblib
- **Data warehouse:** Snowflake
- **Streaming / messaging:** RabbitMQ, pika
- **HTTP / configuration:** requests, python-dotenv
- **Testing / CI:** pytest, GitHub Actions

## Repository structure

```text
freshcart-pipelines/
├── .github/workflows/ci.yml       # GitHub Actions test workflow
├── elt/
│   ├── extract.py                 # API extraction and landing files
│   ├── mock_data.py               # Sample source responses
│   ├── run_pipeline.py            # ELT orchestration and quality checks
│   ├── snowflake_io.py            # Snowflake connection, load and SQL helpers
│   ├── sql/                       # Warehouse setup and transformations
│   └── freshcart_elt_colab.ipynb  # Colab walkthrough
├── iris_project/
│   ├── data_loaders/
│   ├── transformers/
│   ├── data_exporters/
│   ├── pipelines/iris_classifier/
│   └── Mage_ML_PipeLine.ipynb
├── stream/
│   ├── dispatcher.py              # Simulated topic publisher
│   ├── driver.py                  # Topic-pattern consumer
│   ├── freshcart_order_publisher.py
│   ├── stream_to_lakehouse.py     # RabbitMQ -> Snowflake integration
│   └── *.ipynb                    # Streaming walkthroughs
├── tests/
│   └── test_prepare_data.py
├── requirements.txt
└── README.md
```

## Getting started

### 1. Clone and create an environment

```bash
git clone https://github.com/AK-CODE-droid/freshcart-pipelines.git
cd freshcart-pipelines

python3.12 -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell, activate with:

```powershell
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The ELT subproject also has its own dependency file:

```bash
pip install -r elt/requirements.txt
```

### 3. Run tests

```bash
pytest
```

## 1. FreshCart ELT pipeline

### Pipeline stages

1. **Extract:** fetch web orders, mobile orders, CRM tier events, FX rates, and pincode details. API pagination and transient HTTP retries are supported.
2. **Land:** write source records as gzipped JSONL under `landing/<source>/<run-date>/`.
3. **Load:** upload files to the Snowflake internal stage and copy them into bronze tables.
4. **Transform:** run SQL scripts to cleanse and deduplicate orders, maintain customer history (SCD Type 2), update FX and pincode references, and publish the gold fact table.
5. **Validate:** run data-quality checks and report gold order, revenue, and loyalty-point totals.

### Run with sample data (no external source API credentials)

From the repository root:

```bash
cd elt
python run_pipeline.py --dry-run --mock
```

This exercises extraction and writes local landing files without connecting to Snowflake.

To create the Snowflake objects and run the pipeline with sample API responses:

```bash
python run_pipeline.py --setup --mock
```

Subsequent sample runs:

```bash
python run_pipeline.py --mock
```

For real API sources, configure the environment variables below and run:

```bash
python run_pipeline.py
```

Useful options:

| Option | Description |
|---|---|
| `--setup` | Run `sql/00_setup.sql` before loading data |
| `--mock` | Use built-in sample API responses |
| `--dry-run` | Extract and land only; do not connect to Snowflake |
| `--run-date YYYY-MM-DD` | Set the logical processing date |
| `--strict` | Treat quality warnings as failures |

### Snowflake SQL sequence

The orchestrator runs the SQL files in this order:

- `00_setup.sql` — create and configure warehouse objects when `--setup` is supplied.
- `10_silver_orders_clean.sql` — cleanse and validate raw orders.
- `20_silver_orders_dedup.sql` — deduplicate orders.
- `30_dim_customers_scd2.sql` — maintain customer dimension history.
- `31_ref_fx_daily.sql` — prepare daily currency conversion reference data.
- `32_ref_pincode_geo.sql` — prepare pincode geography reference data.
- `40_gold_fct_orders.sql` — enrich and publish the gold order fact.

### ELT environment variables

Create an `.env` file inside `elt/` (or export variables in your shell). Do not commit real credentials.

| Variable | Purpose |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Password authentication (when not using key-pair or SSO) |
| `SNOWFLAKE_ROLE` | Optional Snowflake role |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse |
| `SNOWFLAKE_DATABASE` | Snowflake database |
| `SNOWFLAKE_PRIVATE_KEY_FILE` | Optional private-key file for key-pair authentication |
| `SNOWFLAKE_PRIVATE_KEY_FILE_PWD` | Optional private-key passphrase |
| `SNOWFLAKE_AUTHENTICATOR` | Optional authenticator, e.g. `externalbrowser` |
| `WEB_ORDERS_URL` | Web orders API endpoint |
| `WEB_ORDERS_TOKEN` | Optional bearer token for web orders API |
| `MOBILE_ORDERS_URL` | Mobile orders API endpoint |
| `MOBILE_ORDERS_TOKEN` | Optional bearer token for mobile orders API |
| `CRM_TIER_EVENTS_URL` | CRM events API endpoint |
| `CRM_TOKEN` | Optional bearer token for CRM API |
| `FX_URL` | Optional FX API base URL (defaults to Frankfurter) |
| `FX_BASES` | Comma-separated base currencies (defaults to `USD`) |
| `FX_LOOKBACK_DAYS` | FX lookback window (defaults to `35`) |
| `PINCODE_URL` | Optional pincode API base URL |
| `LANDING_DIR` | Local landing directory (defaults to `landing`) |
| `QUARANTINE_MAX_RATE` | Maximum warning threshold for quarantined records (defaults to `0.05`) |

Example template (replace placeholders with your own values):

```dotenv
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database

WEB_ORDERS_URL=https://your-api.example.com/orders
WEB_ORDERS_TOKEN=
MOBILE_ORDERS_URL=https://your-api.example.com/mobile/orders
MOBILE_ORDERS_TOKEN=
CRM_TIER_EVENTS_URL=https://your-api.example.com/crm/events
CRM_TOKEN=
```

The ELT code supports password, key-pair, and authenticator-based Snowflake login. Use only the authentication variables for the method configured on your account.

## 2. Iris machine-learning pipeline

The Iris project is organized as Mage data-loader, transformer, and data-exporter blocks.

- `load_iris.py` loads the Iris dataset.
- `validate_iris.py` validates the incoming dataset.
- `prepare_data.py` creates a reproducible 80/20 train/test split using `random_state=42`.
- `train_model.py` trains the classifier.
- `evaluate_model.py` evaluates the trained model.
- `save_results.py` exports the results.

The pipeline configuration sets `max_depth: 3`. Open `iris_project/Mage_ML_PipeLine.ipynb` for the notebook walkthrough. The Mage project blocks are intended to run in a Mage environment; the root requirements file alone does not install Mage.

## 3. RabbitMQ streaming

### Start RabbitMQ

Start a local RabbitMQ broker with the management plugin enabled, or use a hosted broker. The scripts default to:

```text
amqp://guest:guest@localhost:5672/%2F
```

For a hosted broker, set `RABBITMQ_URL` in your environment. Keep credentials out of source control.

### Run the dispatcher and drivers

Open separate terminals from the repository root.

Terminal 1 — start a driver:

```bash
python stream/driver.py Ravi 'bangalore.koramangala.*'
```

Terminal 2 — start another driver:

```bash
python stream/driver.py Meena 'bangalore.*.food'
```

Terminal 3 — start a broad subscriber:

```bash
python stream/driver.py Control 'bangalore.#'
```

Terminal 4 — publish simulated requests:

```bash
python stream/dispatcher.py
```

The dispatcher publishes to the durable `rapido-dispatch` topic exchange every two seconds by default. Every tenth message is intentionally malformed (missing `zone`) so the consumers demonstrate negative acknowledgement. Drivers acknowledge valid messages and reject invalid JSON or messages without a zone.

Optional dispatcher arguments include `--interval`, `--bad-every`, `--proof-key city.zone.type`, and `--amqp-url`. Both dispatcher and driver also accept `--amqp-url` or the `RABBITMQ_URL` environment variable.

### Stream FreshCart orders into Snowflake

The integration script is `stream/stream_to_lakehouse.py`. It requires a working RabbitMQ broker, the ELT dependencies, and Snowflake credentials. It takes the ELT directory explicitly:

```bash
python stream/stream_to_lakehouse.py --elt-dir ./elt --seconds 60
```

Publish FreshCart order events to the `freshcart-orders` exchange with routing keys matching `orders.web.#` before or while the consumer runs. The consumer writes collected events to the ELT landing format, loads bronze, executes the silver/gold SQL transformations, and prints before/after gold totals.

To replay a previously created landing file without consuming RabbitMQ:

```bash
python stream/stream_to_lakehouse.py --elt-dir ./elt --replay path/to/landing-file.json.gz
```

The repository also includes Colab walkthroughs under `stream/` and a bonus streaming/lakehouse notebook.

## Tests and continuous integration

Run the test suite locally with:

```bash
pytest
```

GitHub Actions is configured in `.github/workflows/ci.yml`. On pushes and pull requests targeting `main` or `master`, it sets up Python 3.12, installs `requirements.txt`, and runs pytest.

## Configuration and security

- Never commit `.env`, passwords, API tokens, private keys, or broker connection strings.
- Use environment variables or a local ignored `.env` file for credentials.
- Use mock mode to explore the ELT extraction stage without live source APIs.
- The `--dry-run` option does not require Snowflake because the connector is imported only when a warehouse load is requested.
- Keep generated landing data and model artifacts out of version control.

## Author

**AK-CODE-droid** · [GitHub profile](https://github.com/AK-CODE-droid)

---

If you find this project useful, feel free to star the repository or open an issue with questions and suggestions.
