import sys
import os

# 1. Setup path to find 'tests' module (Go up 2 levels: pipeline -> smoke -> root)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. Load the shared fixtures ONLY for this directory
pytest_plugins = ["tests.conftest"]