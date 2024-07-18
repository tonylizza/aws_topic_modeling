# Variables
PYTHON=python3
PIP=pip3
VENV_DIR=venv
REQUIREMENTS=scripts/requirements.txt

# Default target
all: setup test

# Create a virtual environment
setup:
    $(PYTHON) -m venv $(VENV_DIR)
    . $(VENV_DIR)/bin/activate && $(PIP) install -r $(REQUIREMENTS)

# Run tests
test: setup
    . $(VENV_DIR)/bin/activate && $(PYTHON) -m unittest discover -s tests

# Create the RDS instance
create-rds-instance: setup
    . $(VENV_DIR)/bin/activate && terraform -chdir=terraform apply -auto-approve

# Create the database schema
create-schema: setup
    . $(VENV_DIR)/bin/activate && $(PYTHON) scripts/create_schema.py

# Load data into the RDS database
load-data: setup
    . $(VENV_DIR)/bin/activate && $(PYTHON) scripts/load_data.py

# Delete the RDS instance
delete-rds-instance: setup
    . $(VENV_DIR)/bin/activate && terraform -chdir=terraform destroy -auto-approve

# Clean up
clean:
    rm -rf $(VENV_DIR)
