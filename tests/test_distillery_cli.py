import subprocess
import sys
from pathlib import Path

import pytest

def run_distillery_cli(*args):
    cmd = [sys.executable, str(Path(__file__).parents[1] / "distillery.py"), *args]
    return subprocess.run(cmd, capture_output=True, text=True)

def test_help_shows_usage():
    result = run_distillery_cli("--help")
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
    assert "validate" in result.stdout
    assert "execute" in result.stdout

def test_missing_subcommand_shows_error():
    result = run_distillery_cli()
    assert result.returncode != 0
    assert "usage:" in result.stderr.lower() or "usage:" in result.stdout.lower()
    assert "the following arguments are required: command" in (result.stderr + result.stdout).lower()

def test_validate_help_works():
    result = run_distillery_cli("validate", "--help")
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
    assert "--context" in result.stdout

def test_execute_help_works():
    result = run_distillery_cli("execute", "--help")
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
    assert "--context" in result.stdout

def test_validate_requires_context():
    result = run_distillery_cli("validate")
    assert result.returncode != 0
    assert "the following arguments are required: --context" in (result.stderr + result.stdout).lower()

def test_execute_requires_context():
    result = run_distillery_cli("execute")
    assert result.returncode != 0
    assert "the following arguments are required: --context" in (result.stderr + result.stdout).lower()
