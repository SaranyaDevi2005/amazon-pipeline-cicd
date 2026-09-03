# Git Branching Strategy for Amazon Data Pipeline

## Overview

This document defines the Git branching strategy for the Amazon data engineering pipeline. It follows a simplified **Git Flow** model adapted for data engineering teams.

## Branch Model

```
                    ┌────────────────────────────────────────────────┐
                    │                                                │
  main ──────────────●───────────●───────────●───────────●───────▶ PRODUCTION
                        \         /           /           /
  develop ───────────────●───────●───●───────●───────────●───────▶ STAGING
                           \         / \         /
  feature/XYZ ────●───────●───●    /           /
  feature/ABC ────────────────●───●           /
  hotfix/fix-bug ────────────────────────────●
```

## Branch Types

| Branch | Purpose | Naming Convention | Merges Into | Lifetime |
| --- | --- | --- | --- | --- |
| `main` | Production-ready code | `main` | — | Permanent |
| `develop` | Integration/staging | `develop` | `main` (via PR) | Permanent |
| `feature/*` | New features/transforms | `feature/add-discount-validation` | `develop` (via PR) | Temporary |
| `hotfix/*` | Emergency production fixes | `hotfix/fix-silver-merge-bug` | `main` AND `develop` | Temporary |
| `release/*` | Release preparation | `release/v1.0.0` | `main` (via PR) | Temporary |

## Workflow

### 1. Starting New Work
```bash
git checkout develop
git pull origin develop
git checkout -b feature/add-new-transformation
```

### 2. Committing Changes
```bash
git add .
git commit -m "feat: add price bucket classification logic"
```

### 3. Pushing and Creating a Pull Request
```bash
git push origin feature/add-new-transformation
# Create a PR on GitHub: feature/add-new-transformation → develop
```

### 4. CI/CD Pipeline Triggers
- **On PR opened**: Runs unit tests (blocks merge if tests fail)
- **On PR merged to develop**: Deploys to dev environment
- **On release published**: Deploys to production

## Commit Message Conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/):

| Type | Description | Example |
| --- | --- | --- |
| `feat` | New feature/transformation | `feat: add discount_pct calculation to silver layer` |
| `fix` | Bug fix | `fix: correct try_cast for malformed price values` |
| `test` | Test additions | `test: add unit tests for validation rules` |
| `refactor` | Code refactoring | `refactor: extract transformation logic into reusable functions` |
| `docs` | Documentation | `docs: update branching strategy` |
| `ci` | CI/CD changes | `ci: add GitHub Actions workflow for test automation` |
| `chore` | Maintenance | `chore: upgrade Databricks CLI version` |

## Data Engineering Specific Practices

1. **Never modify Bronze tables directly** — always modify the transformation logic in the notebook
2. **Test transformations against mock data** — use the unit test notebook before pushing
3. **Use feature flags** for risky transformations — add a config variable to enable/disable
4. **Version your schema** — if you change table schemas, create a migration script
5. **Run tests locally first** — `databricks bundle run -t dev amazon_pipeline_tests`
6. **Tag releases** — `git tag v1.0.0` for production releases

## Environment Promotion

```
feature branch → develop (auto-deploy to dev) → main/release (deploy to prod)
     ↑                    ↑                          ↑
  Unit tests run     Integration tests        Production deployment
  on PR              on merge                  on release tag
```