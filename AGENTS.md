# Repository Guidelines

## Project Structure & Module Organization
- Python scripts live at the repo root: examples include `HM_MATRIZTRANSACCIONPOSMACROGIRO*.py` and `MATRIZVARIABLES_*`. Each script generates or validates EDV matrices.
- For shared logic, place reusable helpers in a `utils/` folder and import them; include an `__init__.py` if you add packages.
- Put tests under `tests/`, mirroring script names (e.g., `tests/test_matrizvariables_hm_matriztransaccioncajero.py`).

## Build, Test, and Development Commands
- Python: 3.10+ recommended.
- Create a virtual environment (Windows):
  - `python -m venv .venv`
  - `.venv\Scripts\activate`
  - `pip install -r requirements.txt` (if present)
- Run a script locally: `python HM_MATRIZTRANSACCIONPOSMACROGIRO_EDV.py` (adjust filename as needed).
- Run tests: `pytest -q` (once tests are added).
- Lint/format (if configured): `ruff check .` and `black .`.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation and a ~100 character soft wrap.
- Names: functions/variables `snake_case`, classes `CamelCase`, constants `UPPER_CASE`.
- Files: preserve existing prefixes (`HM_`, `MATRIZVARIABLES_`) for scripts extending the same families; otherwise use `snake_case.py`.
- Add concise docstrings (purpose, inputs, outputs). Keep helpers pure and place them in `utils/`.

## Testing Guidelines
- Use `pytest`. Name tests `test_*.py` and co-locate fixtures under `tests/`.
- Prefer small, deterministic sample data; avoid external systems in unit tests.
- Cover core transformations and edge cases (empty frames, nulls, encoding, locale).

## Commit & Pull Request Guidelines
- Commits: small, focused, imperative mood. Prefer Conventional Commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`.
- PRs: include a clear description, motivation, summary of changes, validation steps, and linked issues. Attach sample outputs or screenshots when useful.

## Security & Configuration Tips
- Do not hardcode credentials or absolute paths. Load from environment variables or config files ignored by VCS.
- Be mindful of Windows paths and encodings; use `pathlib` and specify `encoding="utf-8"` for I/O.
- Large datasets and generated outputs should not be committed; add them to `.gitignore`.

