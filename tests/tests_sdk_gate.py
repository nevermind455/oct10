"""Offline control-flow tests; these never substitute for native SDK checks."""
import testing_env  # noqa: F401

import contextlib
import importlib.metadata
import io
import subprocess
import unittest
from unittest.mock import Mock, patch

import run_tests as R


class NativeSdkGate(unittest.TestCase):
    def test_missing_distribution_cannot_pass_strict_check(self):
        with patch.object(R.importlib.metadata, "version", side_effect=importlib.metadata.PackageNotFoundError), \
             patch.object(R, "_run_suites") as run, contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(R.main(["--require-native-sdk", "unified_sdk"]), 1)
        run.assert_not_called()
        self.assertIn("not installed", err.getvalue())
        self.assertIn("did not run", err.getvalue())

    def test_wrong_installed_version_does_not_silently_skip(self):
        with patch.object(R.importlib.metadata, "version", return_value="0.12.0"):
            self.assertIn("installed version is 0.12.0", R.native_sdk_problem())

    def test_broken_dependency_is_reported(self):
        def load(name):
            if name == "eth_account.messages":
                raise ImportError("missing signing dependency")
            return Mock()
        with patch.object(R.importlib.metadata, "version", return_value="0.13.0"), \
             patch.object(R.importlib, "import_module", side_effect=load):
            self.assertIn("eth_account.messages failed: ImportError", R.native_sdk_problem())

    def test_strict_selection_always_includes_the_native_suite(self):
        with patch.object(R, "native_sdk_problem", return_value=None), \
             patch.object(R, "_run_suites", return_value=0) as run:
            self.assertEqual(R.main(["--require-native-sdk", "hybrid_sizing"]), 0)
        suites = run.call_args.args[0]
        self.assertIn(R.TESTS / "tests_unified_sdk.py", suites)
        self.assertTrue(run.call_args.kwargs["require_native_sdk"])

    def test_successful_process_with_sdk_skips_fails_strict_validation(self):
        result = subprocess.CompletedProcess([], 0, "", "Ran 36 tests in 0.1s\n\nOK (skipped=3)\n")
        with patch.object(R.subprocess, "run", return_value=result), \
             contextlib.redirect_stdout(io.StringIO()) as out:
            code = R._run_suites([R.TESTS / "tests_unified_sdk.py"], {}, require_native_sdk=True)
        self.assertEqual(code, 1)
        self.assertIn("strict validation cannot pass", out.getvalue())

    def test_no_skip_sdk_result_is_accepted(self):
        result = subprocess.CompletedProcess([], 0, "", "Ran 36 tests in 0.1s\n\nOK\n")
        with patch.object(R.subprocess, "run", return_value=result), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(R._run_suites([R.TESTS / "tests_unified_sdk.py"], {}, require_native_sdk=True), 0)


if __name__ == "__main__":
    unittest.main()
