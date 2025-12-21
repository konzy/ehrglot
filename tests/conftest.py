"""Pytest configuration and fixtures."""

from pathlib import Path

import pytest


@pytest.fixture
def schema_dir() -> Path:
    """Return path to test schemas directory."""
    return Path(__file__).parent.parent.parent / "schemas"


@pytest.fixture
def sample_patient_data() -> list[dict]:
    """Sample patient data for testing."""
    return [
        {
            "PAT_ID": 1,
            "PAT_MRN_ID": "MRN001",
            "SSN": "123-45-6789",
            "PAT_FIRST_NAME": "John",
            "PAT_LAST_NAME": "Doe",
            "SEX_C": 1,
            "BIRTH_DATE": "1985-03-15",
            "EMAIL_ADDRESS": "john.doe@example.com",
            "HOME_PHONE": "5551234567",
            "ADD_LINE_1": "123 Main St",
            "CITY": "Boston",
            "STATE_C": "MA",
            "ZIP": "02101",
        },
        {
            "PAT_ID": 2,
            "PAT_MRN_ID": "MRN002",
            "SSN": "987-65-4321",
            "PAT_FIRST_NAME": "Jane",
            "PAT_LAST_NAME": "Smith",
            "SEX_C": 2,
            "BIRTH_DATE": "1990-07-22",
            "EMAIL_ADDRESS": "jane.smith@example.com",
            "HOME_PHONE": "5559876543",
            "ADD_LINE_1": "456 Oak Ave",
            "CITY": "Cambridge",
            "STATE_C": "MA",
            "ZIP": "02139",
        },
    ]
