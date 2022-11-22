#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export PYTHONPATH=$SCRIPT_DIR:$PYTHONPATH

python3 -m unittest discover -s tests -t $SCRIPT_DIR
