#!/bin/bash

ENV_NAME="faast-env"

pip install virtualenv --upgrade

# Create a virtual environment
if [ ! -d "$ENV_NAME" ]; then
  echo "Creating virtual environment '$ENV_NAME'..."
  python3 -m venv "$ENV_NAME"
  echo "Virtual environment '$ENV_NAME' created."
else
  echo "Virtual environment '$ENV_NAME' already exists. Skipping creation." 
fi

if [[ $VIRTUAL_ENV != *"$ENV_NAME"* ]]; then
  echo "Activating virtual environment '$ENV_NAME'..."
  source $ENV_NAME/bin/activate
  echo "Virtual environment '$ENV_NAME' activated."
else
  echo "Virtual environment '$ENV_NAME' already activated. Skipping activation."
fi

pip install setuptools --upgrade
pip install wheel --upgrade

# Install dependencies from requirements.txt
pip install -r requirements.txt

(cd util && pip install .)
(cd tests && pip install .)

chmod +x ./start.sh ./stop.sh
