#!/usr/bin/env python3
from unittest.mock import Mock, MagicMock, patch
import os
import sys
import pytest

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from file_validator import FileValidator
from common.constants import (
    FILE_NAME_DEFAULT, FILE_SIZE_DEFAULT, MD5_DEFAULT, FILE_NAME_FIELD, 
    FILE_MD5_FIELD, PRE_MANIFEST, UPLOAD_TYPE, TYPE_FILE, FILE_DIR,
    FROM_S3, ARCHIVE_MANIFEST, FILE_ID_FIELD, FILE_ID_DEFAULT
)


@pytest.fixture
def mock_configs():
    """Fixture for mock configs"""
    return {
        UPLOAD_TYPE: TYPE_FILE,
        FILE_NAME_FIELD: 'file_name',
        FILE_MD5_FIELD: 'md5sum',
        FILE_ID_FIELD: 'file_id',
        PRE_MANIFEST: 'test_manifest.tsv',
        FILE_DIR: '/tmp/test_files',
        FROM_S3: False,
        ARCHIVE_MANIFEST: None
    }


@pytest.fixture
def validator(mock_configs):
    """Fixture for FileValidator instance"""
    with patch('file_validator.get_logger'):
        validator_instance = FileValidator(mock_configs)
        validator_instance.log = Mock()
    return validator_instance


class TestValidateFileName:
    """Unit tests for FileValidator.validate_file_name method"""

    def test_validate_file_name_all_valid(self, validator):
        """Test validation passes when all file names are valid"""
        validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file2.txt', 'md5sum': 'def456'},
            {'file_name': 'nested/file3.txt', 'md5sum': 'ghi789'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should return True for valid file names"
        validator.log.error.assert_not_called()

    def test_validate_file_name_empty_name(self, validator):
        """Test validation fails when file name is empty"""
        validator.manifest_rows = [
            {'file_name': '', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False for empty file name"
        validator.log.error.assert_called()
        error_msg = validator.log.error.call_args[0][0]
        assert "File name is empty" in error_msg
        assert "Line 2" in error_msg

    def test_validate_file_name_whitespace_only(self, validator):
        """Test validation fails when file name contains only whitespace"""
        validator.manifest_rows = [
            {'file_name': '   ', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False for whitespace-only file name"
        error_msg = validator.log.error.call_args[0][0]
        assert "File name is empty" in error_msg

    def test_validate_file_name_duplicate_same_md5(self, validator):
        """Test validation passes when duplicate file names have same md5"""
        validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},  # Same name, same md5
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should pass when duplicate names have same md5"

    def test_validate_file_name_duplicate_different_md5(self, validator):
        """Test validation fails when file name is not unique (different md5s)"""
        validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file1.txt', 'md5sum': 'def456'},  # Same name, different md5
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False for non-unique file names with different md5s"
        error_msg = validator.log.error.call_args[0][0]
        assert "is not unique" in error_msg
        assert "Line 3" in error_msg

    def test_validate_file_name_absolute_path_unix(self, validator):
        """Test validation fails for absolute Unix path"""
        validator.manifest_rows = [
            {'file_name': '/usr/local/file.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False for absolute Unix path"
        error_msg = validator.log.error.call_args[0][0]
        assert "is invalid" in error_msg
        assert "no absolute path allowed" in error_msg

    def test_validate_file_name_reserved_character_asterisk(self, validator):
        """Test validation fails for file name with reserved character *"""
        validator.manifest_rows = [
            {'file_name': 'file*.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False for file name with asterisk"
        error_msg = validator.log.error.call_args[0][0]
        assert "contains invalid characters" in error_msg

    def test_validate_file_name_reserved_character_pipe(self, validator):
        """Test validation fails for file name with reserved character |"""
        validator.manifest_rows = [
            {'file_name': 'file|name.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False for file name with pipe"
        error_msg = validator.log.error.call_args[0][0]
        assert "contains invalid characters" in error_msg

    def test_validate_file_name_valid_special_characters(self, validator):
        """Test validation passes for valid special characters (-, _, .)"""
        validator.manifest_rows = [
            {'file_name': 'file-name_v1.0.txt', 'md5sum': 'abc123'},
            {'file_name': 'data_2024-01-01.csv', 'md5sum': 'def456'},
            {'file_name': 'report.final.pdf', 'md5sum': 'ghi789'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should pass for file names with valid special characters"

    def test_validate_file_name_with_forward_slash_in_name(self, validator):
        """Test validation passes for file names with forward slashes (paths)"""
        validator.manifest_rows = [
            {'file_name': 'subfolder/file.txt', 'md5sum': 'abc123'},
            {'file_name': 'data/2024/file.txt', 'md5sum': 'def456'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should pass for file names with relative paths"

    def test_validate_file_name_line_numbers(self, validator):
        """Test that error messages show correct line numbers"""
        validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': '', 'md5sum': 'def456'},  # Error on line 3
            {'file_name': 'file3.txt', 'md5sum': 'ghi789'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result
        error_msg = validator.log.error.call_args[0][0]
        assert "Line 3" in error_msg

    def test_validate_file_name_multiple_errors(self, validator):
        """Test that all errors are logged even when multiple issues exist"""
        validator.manifest_rows = [
            {'file_name': '', 'md5sum': 'abc123'},  # Empty name
            {'file_name': 'file*name.txt', 'md5sum': 'def456'},  # Invalid char
            {'file_name': '/absolute/path.txt', 'md5sum': 'ghi789'},  # Absolute path
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should return False with multiple errors"
        # Check that multiple error logs were called
        assert validator.log.error.call_count == 3

    def test_validate_file_name_unicode_characters(self, validator):
        """Test validation passes for file names with unicode characters"""
        validator.manifest_rows = [
            {'file_name': 'αρχείο.txt', 'md5sum': 'ghi789'}
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should pass for file names with unicode characters"
    
    def test_validate_file_name_non_unicode_characters(self, validator):
        """Test validation passes for file names with unicode characters"""
        # Construct an unpaired surrogate (low surrogate) character which
        # will raise on utf-8 encoding in Python 3. This reliably simulates
        # a non-unicode/invalid filename for the validator's check.
        bad_char = chr(0xDC80)
        validator.manifest_rows = [
            {'file_name': bad_char + 'a.txt', 'md5sum': 'ghi789'}
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result, "Should not pass for file names with non-unicode characters"

    def test_validate_file_name_long_filename(self, validator):
        """Test validation passes for very long file names"""
        long_name = 'a' * 251 + '.txt'
        validator.manifest_rows = [
            {'file_name': long_name, 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should pass for long file names"

    def test_validate_file_name_multiple_duplicates(self, validator):
        """Test validation fails when multiple file names have duplicates"""
        validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
            {'file_name': 'file1.txt', 'md5sum': 'def456'},  # Duplicate with different md5
            {'file_name': 'file2.txt', 'md5sum': 'ghi789'},
            {'file_name': 'file2.txt', 'md5sum': 'jkl012'},  # Another duplicate
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert not result
        # Should have errors logged for both duplicate files
        assert validator.log.error.call_count >= 2

    def test_validate_file_name_empty_manifest(self, validator):
        """Test validation passes for empty manifest"""
        validator.manifest_rows = []
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "Should pass for empty manifest"

    def test_validate_file_name_info_logging(self, validator):
        """Test that info messages are logged"""
        validator.manifest_rows = [
            {'file_name': 'file1.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result
        # Verify info logging was called
        info_calls = [call for call in validator.log.info.call_args_list]
        assert len(info_calls) >= 2
        
        # Check for expected info messages
        first_info = validator.log.info.call_args_list[0][0][0]
        assert "Start validating file names" in first_info
        
        last_info = validator.log.info.call_args_list[-1][0][0]
        assert "Completed validating file names" in last_info

    def test_validate_file_name_single_valid_file(self, validator):
        """Test validation with single valid file"""
        validator.manifest_rows = [
            {'file_name': 'single_file.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result

    def test_validate_file_name_case_sensitive_duplicates(self, validator):
        """Test validation with case-sensitive file name duplicates"""
        validator.manifest_rows = [
            {'file_name': 'File.txt', 'md5sum': 'abc123'},
            {'file_name': 'file.txt', 'md5sum': 'def456'},  # Different case, different md5
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        # These should be considered different file names (case-sensitive)
        assert result, "Should pass for case-sensitive different names"


class TestValidateFileNameEdgeCases:
    """Edge case tests for FileValidator.validate_file_name method"""

    def test_validate_file_name_backslash_in_name(self, validator):
        """Test validation with backslashes in file name"""
        validator.manifest_rows = [
            {'file_name': 'folder\\file.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        # Backslash is not reserved according to the code
        assert result, "Backslash in relative path should pass"

    def test_validate_file_name_only_extension(self, validator):
        """Test validation with file that is only extension"""
        validator.manifest_rows = [
            {'file_name': '.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "File with only extension should pass"

    def test_validate_file_name_no_extension(self, validator):
        """Test validation with file that has no extension"""
        validator.manifest_rows = [
            {'file_name': 'filename', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "File without extension should pass"

    def test_validate_file_name_space_in_name(self, validator):
        """Test validation with spaces in file name"""
        validator.manifest_rows = [
            {'file_name': 'my file name.txt', 'md5sum': 'abc123'},
        ]
        validator.configs[FILE_NAME_FIELD] = 'file_name'
        validator.configs[FILE_MD5_FIELD] = 'md5sum'
        
        result = validator.validate_file_name()
        
        assert result, "File names with spaces should pass"
