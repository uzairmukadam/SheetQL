import json
import unittest
from unittest.mock import MagicMock, patch

from sheetql.update_check import (
    UpdateCheckOutcome,
    _compare_versions,
    _version_tuple,
    fetch_latest_release_tag,
    run_update_check,
)


class TestVersionParsing(unittest.TestCase):
    def test_version_tuple_plain(self) -> None:
        self.assertEqual(_version_tuple("4.0.0"), (4, 0, 0))

    def test_version_tuple_v_prefix(self) -> None:
        self.assertEqual(_version_tuple("v4.0.1"), (4, 0, 1))

    def test_version_tuple_prerelease_segment(self) -> None:
        self.assertEqual(_version_tuple("4.0.0-rc1"), (4, 0, 0))

    def test_compare_equal(self) -> None:
        self.assertEqual(_compare_versions("4.0.0", "v4.0.0"), 0)

    def test_compare_newer_remote(self) -> None:
        self.assertEqual(_compare_versions("4.0.0", "v4.0.1"), -1)

    def test_compare_older_remote(self) -> None:
        self.assertEqual(_compare_versions("4.1.0", "v4.0.9"), 1)


class TestFetchLatestReleaseTag(unittest.TestCase):
    def _mock_response(self, payload: dict) -> MagicMock:
        body = json.dumps(payload).encode("utf-8")
        cm = MagicMock()
        cm.__enter__.return_value.read.return_value = body
        return cm

    @patch("sheetql.update_check.urllib.request.urlopen")
    def test_fetch_ok(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._mock_response({"tag_name": "v4.0.1"})
        tag = fetch_latest_release_tag(
            "owner/repo", timeout=1.0, user_agent="Test/1"
        )
        self.assertEqual(tag, "v4.0.1")
        mock_urlopen.assert_called_once()

    @patch("sheetql.update_check.urllib.request.urlopen")
    def test_fetch_missing_tag(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._mock_response({})
        tag = fetch_latest_release_tag(
            "owner/repo", timeout=1.0, user_agent="Test/1"
        )
        self.assertIsNone(tag)

    @patch("sheetql.update_check.urllib.request.urlopen")
    def test_fetch_malformed_tag(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = self._mock_response({"tag_name": "not-a-version"})
        tag = fetch_latest_release_tag(
            "owner/repo", timeout=1.0, user_agent="Test/1"
        )
        self.assertIsNone(tag)

    @patch("sheetql.update_check.urllib.request.urlopen")
    def test_fetch_network_error(self, mock_urlopen: MagicMock) -> None:
        import urllib.error

        mock_urlopen.side_effect = urllib.error.URLError("offline")
        tag = fetch_latest_release_tag(
            "owner/repo", timeout=1.0, user_agent="Test/1"
        )
        self.assertIsNone(tag)


class TestRunUpdateCheck(unittest.TestCase):
    @patch("sheetql.update_check.fetch_latest_release_tag")
    @patch("sheetql.update_check.get_package_version")
    def test_update_available(
        self, mock_ver: MagicMock, mock_fetch: MagicMock
    ) -> None:
        mock_ver.return_value = "4.0.0"
        mock_fetch.return_value = "v4.0.1"
        r = run_update_check(repo="x/y", timeout=1.0)
        self.assertEqual(r.outcome, UpdateCheckOutcome.UPDATE_AVAILABLE)
        self.assertEqual(r.remote_tag, "v4.0.1")

    @patch("sheetql.update_check.fetch_latest_release_tag")
    @patch("sheetql.update_check.get_package_version")
    def test_up_to_date(self, mock_ver: MagicMock, mock_fetch: MagicMock) -> None:
        mock_ver.return_value = "4.0.0"
        mock_fetch.return_value = "v4.0.0"
        r = run_update_check(repo="x/y", timeout=1.0)
        self.assertEqual(r.outcome, UpdateCheckOutcome.UP_TO_DATE)

    @patch("sheetql.update_check.fetch_latest_release_tag")
    @patch("sheetql.update_check.get_package_version")
    def test_up_to_date_newer_local(
        self, mock_ver: MagicMock, mock_fetch: MagicMock
    ) -> None:
        mock_ver.return_value = "4.1.0"
        mock_fetch.return_value = "v4.0.0"
        r = run_update_check(repo="x/y", timeout=1.0)
        self.assertEqual(r.outcome, UpdateCheckOutcome.UP_TO_DATE)

    @patch("sheetql.update_check.fetch_latest_release_tag")
    @patch("sheetql.update_check.get_package_version")
    def test_check_failed(self, mock_ver: MagicMock, mock_fetch: MagicMock) -> None:
        mock_ver.return_value = "4.0.0"
        mock_fetch.return_value = None
        r = run_update_check(repo="x/y", timeout=1.0)
        self.assertEqual(r.outcome, UpdateCheckOutcome.CHECK_FAILED)


if __name__ == "__main__":
    unittest.main()
