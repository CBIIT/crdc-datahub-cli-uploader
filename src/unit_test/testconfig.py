import unittest
import os
import yaml
from bento.common.utils import get_logger
from upload_config import Config, UPLOAD_HELP

class TestConfig(unittest.TestCase):
    def setUp(self):
        config_file = "../../config/test-file-config.yml"
        self.config = Config()
        if config_file and os.path.isfile(config_file):
            with open(config_file) as c_file:
                self.data = yaml.safe_load(c_file)['Config']


    def test_validate_configs(self):
        self.config.data = {}
        result = self.config.validate()
        self.assertFalse(result, msg='config validation failed: empty config should return false!')
        self.config.data = self.data 
        result = self.config.validate()
        self.assertTrue(result, msg='config validation failed: with all valid values, should return true')

if __name__ == '__main__':
    unittest.main()
