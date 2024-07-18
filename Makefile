# Variables
PYTHON=python3
PIP=pip3
VENV_DIR=venv
REQUIREMENTS=scripts/requirements.txt

# Default target
all: setup test

# Create a virtual environment and install dependencies
setup:
	$(PYTHON) -m venv $(VENV_DIR)
	. $(VENV_DIR)/bin/activate && $(PIP) install -r $(REQUIREMENTS)

# Run tests
test: setup
	. $(VENV_DIR)/bin/activate && $(PYTHON) -m unittest discover -s tests

# Create the RDS instance using Terraform
create-rds-instance: setup
	terraform -chdir=terraform init
	terraform -chdir=terraform apply -auto-approve

# Create the database schema
create-schema: setup
	. $(VENV_DIR)/bin/activate && $(PYTHON) scripts/create_schema.py

# Load data into the RDS database
load-data: setup
	. $(VENV_DIR)/bin/activate && $(PYTHON) scripts/load_data.py

# Clean up
clean:
	rm -rf $(VENV_DIR)
