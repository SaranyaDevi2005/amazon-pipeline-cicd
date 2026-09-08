# Source Code

The pipeline notebooks are version-controlled in this repo as source files:

- **Pipeline**: `src/Amazon_Production_Pipeline.py`
- **Unit Tests**: `tests/Amazon_Pipeline_Unit_Tests.py`

## How to sync notebook changes from workspace to repo

If you edit the notebooks in the Databricks workspace UI, re-export to keep the repo in sync:

```bash
databricks workspace export --format SOURCE \
  /Users/71762233042@cit.edu.in/Amazon_Production_Pipeline \
  src/Amazon_Production_Pipeline.py

databricks workspace export --format SOURCE \
  /Users/71762233042@cit.edu.in/Amazon_Pipeline_Unit_Tests \
  tests/Amazon_Pipeline_Unit_Tests.py
```

## How to deploy

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev (uploads notebooks from repo to workspace)
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run -t dev amazon_etl_pipeline

# Run unit tests
databricks bundle run -t dev amazon_pipeline_tests
```
