#!/bin/bash

set -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


function airflow() {
    export AIRFLOW_HOME="${THIS_DIR}/airflow"
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AWS_PROFILE=sandbox
    export AWS_REGION=us-east-1
    export AWS_DEFAULT_REGION=us-east-1
    uvx \
        --with "apache-airflow-providers-amazon" \
        --from "apache-airflow-core>=3.0.2" airflow ${@}
}

function start-airflow() {
    airflow standalone
}

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function install-airflow {
    AIRFLOW_VERSION=3.0.2

    # Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
    # See above for supported versions.
    PYTHON_VERSION="$(uv run -- python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example this would install 3.0.0 with python 3.9: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.9.txt

    uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
}

# print all functions in this file
function help {
    echo "$0 <task> <args>"
    echo "Tasks:"
    compgen -A function | cat -n
}

TIMEFORMAT="Task completed in %3lR"
time ${@:-help}
