#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, MagicMock, patch
import os
import sys
from io import StringIO

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from file_validator import FileValidator
from common.constants import (
    FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, FILE_NAME_FIELD, 
    FILE_MD5_FIELD, PRE_MANIFEST, UPLOAD_TYPE, TYPE_FILE, FILE_DIR,
    FROM_S3, ARCHIVE_MANIFEST, FILE_ID_FIELD, FILE_ID_DEFAULT
)


class TestValidateFileName(unittest.TestCase):
    """Unit tests for FileValidator.validate_file_name method"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_configs = {
            UPLOAD_TYPE: TYPE_FILE,
            FILE_NAME_FIELD: 'file_name',
            FILE_MD5_FIELD: 'md5sum',
            FILE_ID_FIELD: 'file_id',
            PRE_MANIFEST: 'test_manifest.tsv',
            FILE_DIR: '/tmp/test_files',
            FROM_S3: False,
            ARCHIVE_MANIFEST: None
        }
        
        # Mock the logger
        with patch('file_validator.get_logger'):
            self.validator = FileValidator(self.mock_configs)
            self.validator.log = Mock()

    def test_validate_file_name_all_valid(self):
        """Test validation passes when all file names are valid"""
        self.validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file2.txt', 'md5sum': 'def456'},
            {'file_name': 'nested/file3.txt', 'md5sum': 'ghi789'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should return True for valid file names")
        self.validator.log.error.assert_not_called()

    def test_validate_file_name_empty_name(self):
        """Test validation fails when file name is empty"""
        self.validator.manifest_rows = [
            {'file_name': '', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False for empty file name")
        self.validator.log.error.assert_called()
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("File name is empty", error_msg)
        self.assertIn("Line 2", error_msg)

    def test_validate_file_name_whitespace_only(self):
        """Test validation fails when file name contains only whitespace"""
        self.validator.manifest_rows = [
            {'file_name': '   ', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False for whitespace-only file name")
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("File name is empty", error_msg)

    def test_validate_file_name_duplicate_same_md5(self):
        """Test validation passes when duplicate file names have same md5"""
        self.validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},  # Same name, same md5
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should pass when duplicate names have same md5")

    def test_validate_file_name_duplicate_different_md5(self):
        """Test validation fails when file name is not unique (different md5s)"""
        self.validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file1.txt', 'md5sum': 'def456'},  # Same name, different md5
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False for non-unique file names with different md5s")
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("is not unique", error_msg)
        self.assertIn("Line 3", error_msg)

    def test_validate_file_name_absolute_path_unix(self):
        """Test validation fails for absolute Unix path"""
        self.validator.manifest_rows = [
            {'file_name': '/usr/local/file.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False for absolute Unix path")
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("is invalid", error_msg)
        self.assertIn("no absolute path allowed", error_msg)


    def test_validate_file_name_reserved_character_asterisk(self):
        """Test validation fails for file name with reserved character *"""
        self.validator.manifest_rows = [
            {'file_name': 'file*.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False for file name with asterisk")
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("contains invalid characters", error_msg)


    def test_validate_file_name_reserved_character_pipe(self):
        """Test validation fails for file name with reserved character |"""
        self.validator.manifest_rows = [
            {'file_name': 'file|name.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False for file name with pipe")
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("contains invalid characters", error_msg)

    def test_validate_file_name_valid_special_characters(self):
        """Test validation passes for valid special and non-ASCII characters (-, _, .)"""
        self.validator.manifest_rows = [
            {'file_name': 'file-name_v1.0.txt', 'md5sum': 'abc123'},
            {'file_name': 'data_2024-01-01.csv', 'md5sum': 'def456'},
            {'file_name': 'report.finalâ¯PM.pdf', 'md5sum': 'ghi789'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should pass for file names with valid special and non-ASCII characters")
    
    #
    

    def test_validate_file_name_with_forward_slash_in_name(self):
        """Test validation passes for file names with forward slashes (paths)"""
        self.validator.manifest_rows = [
            {'file_name': 'subfolder/file.txt', 'md5sum': 'abc123'},
            {'file_name': 'data/2024/file.txt', 'md5sum': 'def456'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should pass for file names with relative paths")

    def test_validate_file_name_line_numbers(self):
        """Test that error messages show correct line numbers"""
        self.validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': '', 'md5sum': 'def456'},  # Error on line 3
            {'file_name': 'file3.txt', 'md5sum': 'ghi789'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result)
        error_msg = self.validator.log.error.call_args[0][0]
        self.assertIn("Line 3", error_msg)

    def test_validate_file_name_multiple_errors(self):
        """Test that all errors are logged even when multiple issues exist"""
        self.validator.manifest_rows = [
            {'file_name': '', 'md5sum': 'abc123'},  # Empty name
            {'file_name': 'file*name.txt', 'md5sum': 'def456'},  # Invalid char
            {'file_name': '/absolute/path.txt', 'md5sum': 'ghi789'},  # Absolute path
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result, "Should return False with multiple errors")
        # Check that multiple error logs were called
        self.assertEqual(self.validator.log.error.call_count, 3)

    def test_validate_file_name_unicode_characters(self):
        """Test validation passes for file names with unicode characters"""
        self.validator.manifest_rows = [
            {'file_name': 'αρχείο.txt', 'md5sum': 'ghi789'}
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should pass for file names with unicode characters")

    def test_validate_file_name_long_filename(self):
        """Test validation passes for very long file names"""
        long_name = 'a' * 255 + '.txt'
        self.validator.manifest_rows = [
            {'file_name': long_name, 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should pass for long file names")

    def test_validate_file_name_multiple_duplicates(self):
        """Test validation fails when multiple file names have duplicates"""
        self.validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file1.txt', 'md5sum': 'def456'},  # Duplicate with different md5
            {'file_name': 'file2.txt', 'md5sum': 'ghi789'},
            {'file_name': 'file2.txt', 'md5sum': 'jkl012'},  # Another duplicate
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertFalse(result)
        # Should have errors logged for both duplicate files
        self.assertGreaterEqual(self.validator.log.error.call_count, 2)

    def test_validate_file_name_empty_manifest(self):
        """Test validation passes for empty manifest"""
        self.validator.manifest_rows = []
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "Should pass for empty manifest")

    def test_validate_file_name_info_logging(self):
        """Test that info messages are logged"""
        self.validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result)
        # Verify info logging was called
        info_calls = [call for call in self.validator.log.info.call_args_list]
        self.assertGreaterEqual(len(info_calls), 2)
        
        # Check for expected info messages
        first_info = self.validator.log.info.call_args_list[0][0][0]
        self.assertIn("Start validating file names", first_info)
        
        last_info = self.validator.log.info.call_args_list[-1][0][0]
        self.assertIn("Completed validating file names", last_info)

    def test_validate_file_name_single_valid_file(self):
        """Test validation with single valid file"""
        self.validator.manifest_rows = [
            {'file_name': 'single_file.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result)

    def test_validate_file_name_case_sensitive_duplicates(self):
        """Test validation with case-sensitive file name duplicates"""
        self.validator.manifest_rows = [
            {'file_name': 'File.txt', 'md5sum': 'abc123'},
            {'file_name': 'file.txt', 'md5sum': 'def456'},  # Different case, different md5
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        # These should be considered different file names (case-sensitive)
        self.assertTrue(result, "Should pass for case-sensitive different names")


class TestValidateFileNameEdgeCases(unittest.TestCase):
    """Edge case tests for FileValidator.validate_file_name method"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_configs = {
            UPLOAD_TYPE: TYPE_FILE,
            FILE_NAME_FIELD: 'file_name',
            FILE_MD5_FIELD: 'md5sum',
            FILE_ID_FIELD: 'file_id',
            PRE_MANIFEST: 'test_manifest.tsv',
            FILE_DIR: '/tmp/test_files',
            FROM_S3: False,
            ARCHIVE_MANIFEST: None
        }
        
        with patch('file_validator.get_logger'):
            self.validator = FileValidator(self.mock_configs)
            self.validator.log = Mock()

    def test_validate_file_name_backslash_in_name(self):
        """Test validation with backslashes in file name"""
        self.validator.manifest_rows = [
            {'file_name': 'folder\\file.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        # Backslash is not reserved according to the code
        self.assertTrue(result, "Backslash in relative path should pass")

    def test_validate_file_name_only_extension(self):
        """Test validation with file that is only extension"""
        self.validator.manifest_rows = [
            {'file_name': '.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "File with only extension should pass")

    def test_validate_file_name_no_extension(self):
        """Test validation with file that has no extension"""
        self.validator.manifest_rows = [
            {'file_name': 'filename', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "File without extension should pass")

    def test_validate_file_name_space_in_name(self):
        """Test validation with spaces in file name"""
        self.validator.manifest_rows = [
            {'file_name': 'my file name.txt', 'md5sum': 'abc123'},
        ]
        self.validator.configs[FILE_NAME_FIELD] = 'file_name'
        self.validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = self.validator.validate_file_name()
        
        self.assertTrue(result, "File names with spaces should pass")


if __name__ == '__main__':
    unittest.main()
