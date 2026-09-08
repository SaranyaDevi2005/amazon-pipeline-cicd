# Amazon Data Pipeline — CI/CD Project

## Project Structure

```
amazon-pipeline-cicd/
├── .github/
│   └── workflows/
│       └── ci.yml                    # GitHub Actions CI/CD workflow
├── databricks.yml                     # Declarative Automation Bundle (DAB)
├── BRANCHING_STRATEGY.md              # Git branching strategy documentation
├── README.md                          # This file
├── src/
│   ├── README.md                      # Source code guide
│   └── Amazon_Production_Pipeline.py   # Main ETL pipeline (Bronze → Silver → Gold)
└── tests/
    ├── README.md                      # Test guide
    └── Amazon_Pipeline_Unit_Tests.py   # Unit tests for transformation logic
```

## Quick Start

### 1. Connect Your GitHub Account

Go to **Settings → Linked Accounts** in Databricks and connect your GitHub account:
```
https://dbc-28f616d1-12f0.cloud.databricks.com/settings/user/linked-accounts?o=7474651772228655
```

### 2. Clone This Repo as a Git Folder

In Databricks, go to **Repos** and clone your GitHub repository. Or use the API:
```bash
databricks repos create --url https://github.com/<your-username>/amazon-pipeline-cicd --provider gitHub
```

### 3. Set GitHub Actions Secrets

In your GitHub repository, go to **Settings → Secrets and variables → Actions** and add:
- `DATABRICKS_HOST`: Your Databricks workspace URL (e.g., `https://dbc-28f616d1-12f0.cloud.databricks.com`)
- `DATABRICKS_TOKEN`: A Databricks personal access token

### 4. Deploy Using DAB

```bash
# Validate the bundle configuration
databricks bundle validate -t dev

# Deploy to dev environment
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run -t dev amazon_etl_pipeline

# Run unit tests
databricks bundle run -t dev amazon_pipeline_tests
```

## CI/CD Pipeline Stages

| Stage | Trigger | Action |
| --- | --- | --- |
| **Validate Bundle** | Every push/PR | Validates DAB configuration |
| **Run Unit Tests** | Every push/PR | Runs test notebook on Databricks |
| **Deploy to Dev** | Push to `develop` | Deploys bundle to dev environment |
| **Deploy to Prod** | Release published | Deploys bundle to production |

## Unit Tests

The unit test notebook (`Amazon_Pipeline_Unit_Tests`) tests:
- Type casting (string → double with try_cast)
- String cleaning (₹ removal, comma removal)
- Validation rules (null checks, range checks)
- Derived column calculations (discount_amount, discount_pct)
- Deduplication logic
- Dead letter record isolation
- Mock data testing with known inputs and expected outputs

## Branching Strategy

See [BRANCHING_STRATEGY.md](BRANCHING_STRATEGY.md) for the full Git branching strategy.

| Branch | Purpose |
| --- | --- |
| `main` | Production-ready code |
| `develop` | Integration / staging |
| `feature/*` | New features and transformations |
| `hotfix/*` | Emergency production fixes |
| `release/*` | Release preparation |