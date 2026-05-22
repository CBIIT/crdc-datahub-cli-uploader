import os
import sys
import yaml
import pytest
from unittest.mock import patch
from bento.common.utils import get_logger
from upload_config import Config


@pytest.fixture
def config_data():
    """Fixture to load test config data"""
    config_file = "../../config/test-file-config.yml"
    if config_file and os.path.isfile(config_file):
        with open(config_file) as c_file:
            return yaml.safe_load(c_file)['Config']
    return None


class TestConfig:
    """Test suite for Config class"""

    def test_validate_configs_empty(self):
        """Test that empty config validation fails"""
        with patch.object(sys, 'argv', ['uploader']):
            config = Config()
            config.data = {}
            result = config.validate()
            assert not result, "Empty config should return False"

    def test_validate_configs_valid(self, config_data):
        """Test that valid config passes validation"""
        if config_data is None:
            pytest.skip("Test config file not found")
        with patch.object(sys, 'argv', ['uploader']):
            config = Config()
            config.data = config_data
            result = config.validate()
            assert result, "Config with all valid values should return True"
