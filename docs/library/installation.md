# Installation

## Requirements

- Python 3.11+
- pip

---

## Install from PyPI

```bash
pip install csvw-safe
```

## Development Installation

Clone the repository and install development dependencies:

```bash
git clone https://github.com/dscc-admin-ch/csvw-safe-library.git

cd csvw-safe-library

pip install -e .[dev]
```

## Run Tests

```bash
pytest --cov=csvw_safe --cov-report=term-missing tests/
```

## Optional Dependencies

Some features require additional libraries:
| Feature                    | Dependency       |
| -------------------------- | ---------------- |
| SHACL validation           | `pyshacl`        |
| OpenDP integration         | `opendp`         |
| SmartNoise SQL integration | `smartnoise-sql` |

Install optional dependencies manually when needed.