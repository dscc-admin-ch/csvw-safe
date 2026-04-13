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


# Enforce formatting
ruff format src/
ruff format tests/

# ALL Checks
ruff check src/ --fix

# Type checking
python -m mypy src/
