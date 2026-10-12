"""Fast real-Git regression checks for synthetic guest setup and diagnostics."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('host_vm_guest', Path(__file__).with_name('host_vm_guest.py'))
guest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guest)


class GuestSetupTests(unittest.TestCase):
    def test_empty_protocol_v0_clone_gets_explicit_main_before_first_commit(self):
        with tempfile.TemporaryDirectory(prefix='host-vm-git-') as directory:
            root = Path(directory)
            origin, author = root / 'origin.git', root / 'author'
            guest.run(['git', 'init', '--bare', '--initial-branch=main', str(origin)])
            guest.run(['git', '-c', 'protocol.version=0', '-c', 'init.defaultBranch=master',
                       'clone', origin.as_uri(), str(author)])
            self.assertEqual(guest.git(author, 'symbolic-ref', 'HEAD'), 'refs/heads/master')
            guest.initialize_empty_author(author)
            guest.git(author, 'config', 'user.name', 'Fixture')
            guest.git(author, 'config', 'user.email', 'fixture@example.invalid')
            (author / 'program').write_text('harmless baseline\n')
            baseline = guest.commit(author, 'baseline')
            guest.git(author, 'push', 'origin', 'main')
            self.assertEqual(guest.git(origin, 'rev-parse', 'refs/heads/main'), baseline)
            with self.assertRaises(AssertionError):
                guest.initialize_empty_author(author)

    def test_failure_exposes_bounded_captured_streams(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(subprocess.CalledProcessError):
            guest.run([sys.executable, '-c', "import sys;print('B'*3000);sys.stderr.write('A'*5000+'cause');sys.exit(7)"])
        prefix, _, raw = stderr.getvalue().partition(' ')
        self.assertEqual(prefix, 'FIXTURE_COMMAND_FAILURE')
        detail = json.loads(raw)
        self.assertEqual(detail['returncode'], 7)
        self.assertEqual(len(detail['stdout_tail']), 2000)
        self.assertEqual(len(detail['stderr_tail']), 4000)
        self.assertTrue(detail['stderr_tail'].endswith('cause'))

    def test_expected_nonzero_probe_does_not_emit_failure(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            result = guest.run([sys.executable, '-c', 'raise SystemExit(2)'], check=False)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(stderr.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
