# Source Notebooks

The pipeline notebooks are stored in the Databricks workspace and referenced by path in `databricks.yml`:

- **Pipeline**: `/Users/71762233042@cit.edu.in/Amazon_Production_Pipeline`
- **Unit Tests**: `/Users/71762233042@cit.edu.in/Amazon_Pipeline_Unit_Tests`

## How to sync notebook code to this repo

Use the Databricks CLI to export notebooks as source:

```bash
databricks workspace export --format SOURCE \
  /Users/71762233042@cit.edu.in/Amazon_Production_Pipeline \
  src/Amazon_Production_Pipeline.py

databricks workspace export --format SOURCE \
  /Users/71762233042@cit.edu.in/Amazon_Pipeline_Unit_Tests \
  tests/Amazon_Pipeline_Unit_Tests.py
```
