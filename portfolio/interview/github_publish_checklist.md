# GitHub Publish Checklist

## Repository hygiene
- [ ] `README.md` is complete and accurate for current scope.
- [ ] `.env` is ignored and no secrets are committed.
- [ ] `requirements*.txt` are pinned.
- [ ] `dbt/target` and local artifacts are not committed.

## Code quality
- [ ] Python scripts include clear function-level docstrings.
- [ ] SQL models state purpose and grain.
- [ ] dbt models/tests pass in the selected target.
- [ ] Airflow DAGs import cleanly and run end-to-end.

## Documentation
- [ ] `docs/ARCHITECTURE.md`
- [ ] `docs/DATA_DICTIONARY.md`
- [ ] `docs/DECISIONS.md`
- [ ] BI and interview files under `portfolio/`

## Portfolio evidence
- [ ] Dashboard screenshots added in `portfolio/dashboards/screenshots/`
- [ ] Query examples available under `portfolio/sample_queries/`
- [ ] Airflow graph/log screenshots captured.
