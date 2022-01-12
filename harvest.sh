#!/bin/sh
#
# Prerequisite: Set environment variable HARVESTER_HOME to point to the base 
# directory containing the harvester directory and anaconda python.
PY_ENV=harvester
#PY_EXEC=${HARVESTER_HOME}/anaconda3/bin/python
PY_EXEC=${HARVESTER_HOME}/.venv/${PY_ENV}/bin/python
PY_SCRIPT=${HARVESTER_HOME}/harvester/harvest.py
export PYTHONPATH=${HARVESTER_HOME}:${PYTHONPATH}
echo `date` Calling ${PY_EXEC} ${PY_SCRIPT} "$@"
#source ${HARVESTER_HOME}/.venv/${PY_ENV}/bin/activate.csh
exec ${PY_EXEC} ${PY_SCRIPT} "$@"
echo `date` Finished ====================================
