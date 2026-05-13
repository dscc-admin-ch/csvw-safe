# Installation

## Requirements

- Python 3.11+
- pip

---

## Install from PyPI

```bash
pip install csvw-eo
```

## Development Installation

Clone the repository and install development dependencies:

```bash
git clone https://github.com/dscc-admin-ch/csvw-eo-library.git

cd csvw-eo-library

pip install -e .[dev]
```

## Run Tests

```bash
pytest --cov=csvw_eo --cov-report=term-missing tests/
```

## Run linter

```bash
chmod +x run_linter.sh
./run_linter.sh
```

## Optional Dependencies

Some features require additional libraries:

| Feature                    | Dependency       |
| -------------------------- | ---------------- |
| SHACL validation           | `pyshacl`        |
| OpenDP integration         | `opendp`         |

Install optional dependencies manually when needed.