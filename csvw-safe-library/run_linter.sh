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
    pip install -r requirements_dev.txt
fi

python -m isort csvw_safe/
python -m black csvw_safe/
python -m flake8 --toml-config=./pyproject.toml csvw_safe/
python -m mypy csvw_safe/
python -m pylint csvw_safe/
python -m pydocstringformatter -w csvw_safe/

python -m isort tests/
python -m black tests/
python -m flake8 --toml-config=./pyproject.toml tests/
