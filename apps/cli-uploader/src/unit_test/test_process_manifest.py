#!/usr/bin/env python3
"""Unit tests for process_manifest._is_valid_file_id_value."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.constants import DCF_PREFIX, OMIT_DCF_PREFIX
from process_manifest import _is_valid_file_id_value

# UUID that passes common.utils.is_valid_uuid (v5-style parsing)
VALID_UUID = "c9bf9e57-1685-4c89-bafb-ff5af830be8a"


class TestIsValidFileIdValue:
    def test_none_returns_false(self):
        assert _is_valid_file_id_value(None, {}) is False

    def test_empty_string_returns_false(self):
        assert _is_valid_file_id_value("", {}) is False

    def test_whitespace_only_returns_false(self):
        assert _is_valid_file_id_value("   ", {}) is False

    def test_non_string_returns_false(self):
        assert _is_valid_file_id_value(123, {}) is False

    def test_omit_prefix_valid_uuid(self):
        configs = {OMIT_DCF_PREFIX: True}
        assert _is_valid_file_id_value(VALID_UUID, configs) is True

    def test_omit_prefix_valid_uuid_stripped(self):
        configs = {OMIT_DCF_PREFIX: True}
        assert _is_valid_file_id_value(f"  {VALID_UUID}  ", configs) is True

    def test_omit_prefix_invalid_uuid(self):
        configs = {OMIT_DCF_PREFIX: True}
        assert _is_valid_file_id_value("not-a-uuid", configs) is False

    def test_missing_key_defaults_to_require_dcf_prefix(self):
        """When omit key is absent, default False → require dg.4DFC/<uuid> format."""
        assert _is_valid_file_id_value(VALID_UUID, {}) is False
        assert _is_valid_file_id_value(f"{DCF_PREFIX}{VALID_UUID}", {}) is True

    def test_require_dcf_prefix_valid(self):
        configs = {OMIT_DCF_PREFIX: False}
        assert _is_valid_file_id_value(f"{DCF_PREFIX}{VALID_UUID}", configs) is True

    def test_require_dcf_prefix_stripped(self):
        configs = {OMIT_DCF_PREFIX: False}
        assert _is_valid_file_id_value(f"  {DCF_PREFIX}{VALID_UUID}  ", configs) is True

    def test_require_dcf_prefix_missing_prefix(self):
        configs = {OMIT_DCF_PREFIX: False}
        assert _is_valid_file_id_value(VALID_UUID, configs) is False

    def test_require_dcf_prefix_bad_uuid_suffix(self):
        configs = {OMIT_DCF_PREFIX: False}
        assert _is_valid_file_id_value(f"{DCF_PREFIX}not-a-uuid", configs) is False

    def test_require_dcf_prefix_only_prefix(self):
        configs = {OMIT_DCF_PREFIX: False}
        assert _is_valid_file_id_value(DCF_PREFIX.rstrip("/"), configs) is False
        assert _is_valid_file_id_value(f"{DCF_PREFIX}", configs) is False
