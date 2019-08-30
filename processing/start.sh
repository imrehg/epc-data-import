#!/usr/bin/env bash

IMPORT_FILE_PATH="/import/${IMPORT_FILE:-all-domestic-certificates.zip}"
if [ -f "${IMPORT_FILE_PATH}" ]; then
    # Import file exists, let's get on with it.
    if python app/processor.py "${IMPORT_FILE_PATH}" ; then
        echo "Imports succeeded"
        # Only showing this for ergonomy in the exercise not as a standard practice
        echo "Can check results by logging in to the database management at http://localhost:8080 with '$POSTGRES_USER' / '$POSTGRES_PASSWORD'"
    else
        echo "Imports have failed."
    fi
else
    echo "No imports file to handle, exitting."
fi
