# Tests

- Put focused transformation tests in `tests/unit/`.
- Put end-to-end pipeline tests in `tests/integration/`.
- Use small files under `data/sample/` so tests remain reproducible.
- Do not commit large raw or generated datasets.

Run the focused NYC taxi feature checks with:

```bash
python -m pytest -q tests/integration/test_transformation.py
```

Run the full test suite with:

```bash
python -m pytest -q
```
