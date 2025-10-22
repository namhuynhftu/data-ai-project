# GitHub Actions CI Setup Guide

## Overview

This project uses GitHub Actions for Continuous Integration (CI) to automatically test dbt models and lint SQL code on every pull request.

## Workflow: `pr_ci.yml`

### Triggers
- Pull requests to `main`, `develop`, or `feature/*` branches
- Changes to:
  - SQL files in `dwh/snowflake/**/*.sql`
  - YAML files in `dwh/snowflake/**/*.yml`
  - `dbt_project.yml`
  - Workflow file itself

### Steps

1. **Checkout code**: Retrieves the repository code
2. **Setup Python**: Installs Python 3.11
3. **Install uv**: Installs the uv package manager
4. **Cache dependencies**: Caches uv dependencies for faster builds
5. **Install CI dependencies**: Installs only CI-required packages
6. **Run dbt debug**: Verifies dbt configuration and Snowflake connection
7. **Run SQLFluff lint**: Lints SQL code for style and quality
8. **Run dbt deps**: Installs dbt package dependencies
9. **Run dbt compile**: Compiles dbt models to verify syntax
10. **Run dbt tests**: Executes data quality tests
11. **Upload artifacts**: Saves dbt artifacts for review

## Required GitHub Secrets

Configure these in your repository: **Settings → Secrets and variables → Actions**

### Secrets (sensitive data):
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_PRIVATE_KEY_FILE`: Your Snowflake private key content
- `SNOWFLAKE_PRIVATE_KEY_FILE_PWD`: Your private key password

### Variables (non-sensitive):
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_DATABASE`: Your Snowflake database name
- `SNOWFLAKE_WAREHOUSE`: Your Snowflake warehouse name
- `SNOWFLAKE_ROLE`: Your Snowflake role name
- `SNOWFLAKE_SCHEMA`: Your Snowflake schema name (e.g., RAW_DATA)

## Local Testing

Test the CI workflow locally before pushing:

```bash
# Install CI dependencies
uv sync --group pr_ci

# Run SQLFluff
uv run sqlfluff lint dwh/snowflake/models/

# Run dbt debug
cd dwh/snowflake
uv run dbt debug -t ci

# Run dbt tests
uv run dbt test -t ci
```

## Troubleshooting

### Connection Issues
- Verify all secrets and variables are set correctly
- Check Snowflake credentials are valid
- Ensure private key format is correct

### Dependency Issues
- Clear cache by changing workflow file
- Update `pyproject.toml` if needed
- Verify `uv.lock` is committed

### Test Failures
- Review test results in Actions tab
- Check dbt artifacts for detailed errors
- Run tests locally to reproduce

## Best Practices

1. **Always create pull requests** - CI runs automatically
2. **Fix linting issues** before requesting review
3. **Review CI results** before merging
4. **Keep dependencies updated** in `pyproject.toml`
5. **Document model changes** in schema.yml files

## Next Steps

- Add more custom tests in `tests/` directory
- Configure branch protection rules
- Add performance benchmarking
- Implement automated documentation generation
