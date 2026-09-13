# Tests

- Put focused transformation tests in `tests/unit/`.
- Put end-to-end pipeline tests in `tests/integration/`.
- Use small files under `data/sample/` so tests remain reproducible.
- Do not commit large raw or generated datasets.

Run the test suite with:

```bash
pytest -q
```
