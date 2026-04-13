#!/bin/bash

# Parse command line arguments
INSTALL_DEPS=false

for arg in "$@"; do
    case $arg in
        --install-deps)
            INSTALL_DEPS=true
            ;;
    esac
done


# Install dependencies if flag is set
if [ "$INSTALL_DEPS" == true ]; then
    pip install .[dev]
fi


# -------------------------
# Ruff
# -------------------------
# Enforce formatting (if you use ruff formatter instead of black)
ruff format src/
ruff format tests/

# Auto-fix imports + lint issues
ruff check src/ --fix


# -------------------------
# Type checking
# -------------------------
python -m mypy src/


# -------------------------
# Documentation formatting
# -------------------------
python -m pydocstringformatter -w src/
python -m pydocstringformatter -w tests/