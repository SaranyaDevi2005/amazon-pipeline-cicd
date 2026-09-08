# Unit Tests

The unit test notebook is version-controlled in this repo:
- `tests/Amazon_Pipeline_Unit_Tests.py`

## Test Coverage

| # | Test | What it validates |
| --- | --- | --- |
| 1 | Type Casting | `try_cast` safely converts strings to doubles |
| 2 | String Cleaning | ₹ and comma removal from price/rating columns |
| 3 | Validation Rules | Null checks, range checks, is_valid flagging |
| 4 | Derived Columns | discount_amount and discount_pct calculations |
| 5 | Deduplication | Composite key dedup preserves correct row |
| 6 | Dead Letter | Invalid records are correctly isolated |
| 7 | End-to-End | Full transform→validate→dedup pipeline on mock data |

## Running Tests

```bash
# Run via DAB
 databricks bundle run -t dev amazon_pipeline_tests

# Or run the notebook directly in the Databricks workspace
```

Tests run automatically via GitHub Actions CI on every push and pull request.
