"""Shared fixtures for qdrant-manager tests."""
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_client():
    """Create a mock Qdrant client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_models():
    """Create a mock for the models module."""
    with patch('qdrant_manager.cli.models') as mock_models:
        # Set up the Distance enum
        mock_models.Distance = MagicMock()
        mock_models.Distance.COSINE = MagicMock()
        mock_models.Distance.COSINE.name = "COSINE"
        mock_models.Distance.EUCLID = MagicMock()
        mock_models.Distance.EUCLID.name = "EUCLID"
        mock_models.Distance.DOT = MagicMock()
        mock_models.Distance.DOT.name = "DOT"
        
        # Set up vector params
        mock_models.VectorParams = MagicMock()
        mock_models.OptimizersConfigDiff = MagicMock()
        
        yield mock_models


@pytest.fixture
def mock_logger():
    """Create a mock for the logger."""
    with patch('qdrant_manager.cli.logger') as mock_logger:
        yield mock_logger