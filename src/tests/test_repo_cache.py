"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
import os
import tempfile
import shutil
import time
from unittest.mock import patch, MagicMock

from datasurface.cmd.platform import (
    getCachedModelPath, getLatestCachedModel, isModelCacheStale,
    cloneToCache, getCachedOrCloneModel, cleanupOldCachedModels,
    getLatestModelAtTimestampedFolder
)
from datasurface.md.repo import GitHubRepository
from datasurface.md.credential import Credential, CredentialType, CredentialStore
from datasurface.md.lint import ValidationTree


class MockCredentialStore(CredentialStore):
    """Mock credential store for testing"""

    def __init__(self):
        super().__init__("MockCredStore", set())

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        pass  # Mock implementation

    def getAsUserPassword(self, credential: Credential) -> tuple[str, str]:
        return ("mock_user", "mock_password")

    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        return ("mock_public", "mock_private", "mock_password")

    def getAsToken(self, credential: Credential) -> str:
        return "mock_token"

    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        pass  # Mock implementation


class Test_RepoCache(unittest.TestCase):
    """Test git repository caching functionality"""

    def setUp(self):
        """Set up test environment with temporary directories"""
        self.test_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.test_dir, "cache")
        os.makedirs(self.cache_dir, exist_ok=True)

        self.cred_store = MockCredentialStore()
        self.test_repo = GitHubRepository(
            "testowner/testrepo",
            "main",
            credential=Credential("git", CredentialType.API_TOKEN)
        )

    def tearDown(self):
        """Clean up test directories"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_getCachedModelPath(self):
        """Test that cache path generation works correctly"""
        path = getCachedModelPath(self.cache_dir, self.test_repo)
        expected = os.path.join(self.cache_dir, "testowner_testrepo_main")
        self.assertEqual(path, expected)

        # Test with special characters that need replacement
        special_repo = GitHubRepository("test-owner/test-repo", "feature/test-branch")
        path = getCachedModelPath(self.cache_dir, special_repo)
        # Only slash gets replaced in branch names, not dashes
        expected = os.path.join(self.cache_dir, "test_owner_test_repo_feature_test-branch")
        self.assertEqual(path, expected)

    def test_getLatestCachedModel_no_cache(self):
        """Test getting latest cached model when no cache exists"""
        result = getLatestCachedModel(self.cache_dir, self.test_repo)
        self.assertIsNone(result)

    def test_getLatestCachedModel_with_cache(self):
        """Test getting latest cached model when cache exists"""
        # Create mock cached model folders with different timestamps
        repo_cache_dir = getCachedModelPath(self.cache_dir, self.test_repo)
        os.makedirs(repo_cache_dir, exist_ok=True)

        # Create folders with different timestamps
        timestamps = [1234567890, 1234567900, 1234567880]  # middle one is latest
        folders = []
        for ts in timestamps:
            folder = os.path.join(repo_cache_dir, f"model-{ts:010d}")
            os.makedirs(folder, exist_ok=True)
            # Create a dummy eco.py file
            with open(os.path.join(folder, "eco.py"), "w") as f:
                f.write("# Test ecosystem file")
            folders.append(folder)

        latest = getLatestCachedModel(self.cache_dir, self.test_repo)
        expected = os.path.join(repo_cache_dir, "model-1234567900")
        self.assertEqual(latest, expected)

    def test_isModelCacheStale_no_cache(self):
        """Test cache staleness check when no cache exists"""
        fake_path = os.path.join(self.cache_dir, "nonexistent")
        result = isModelCacheStale(fake_path, self.test_repo, self.cred_store, 5)
        self.assertTrue(result)

    def test_isModelCacheStale_fresh_cache(self):
        """Test cache staleness check with fresh cache"""
        # Create a cache folder with current timestamp
        current_time = int(time.time())
        cache_folder = os.path.join(self.cache_dir, f"model-{current_time:010d}")
        os.makedirs(cache_folder, exist_ok=True)

        result = isModelCacheStale(cache_folder, self.test_repo, self.cred_store, 5)
        self.assertFalse(result)  # Should be fresh

    def test_isModelCacheStale_old_cache(self):
        """Test cache staleness check with old cache (mock remote check)"""
        # Create a cache folder with old timestamp (10 minutes ago)
        old_time = int(time.time()) - 600  # 10 minutes ago
        cache_folder = os.path.join(self.cache_dir, f"model-{old_time:010d}")
        os.makedirs(cache_folder, exist_ok=True)

        # Create .git directory to simulate git repo
        git_dir = os.path.join(cache_folder, ".git")
        os.makedirs(git_dir, exist_ok=True)

        # Mock subprocess.run for git ls-remote and git rev-parse
        with patch('datasurface.cmd.platform.subprocess.run') as mock_run:
            # Mock git ls-remote returning a commit hash
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="abc123def456\trefs/heads/main\n"),  # git ls-remote
                MagicMock(returncode=0, stdout="abc123def456\n")  # git rev-parse HEAD
            ]

            result = isModelCacheStale(cache_folder, self.test_repo, self.cred_store, 5)
            self.assertFalse(result)  # Same commit, not stale

        # Test with different commits (stale)
        with patch('datasurface.cmd.platform.subprocess.run') as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="xyz789abc123\trefs/heads/main\n"),  # git ls-remote (different)
                MagicMock(returncode=0, stdout="abc123def456\n")  # git rev-parse HEAD (local)
            ]

            result = isModelCacheStale(cache_folder, self.test_repo, self.cred_store, 5)
            self.assertTrue(result)  # Different commits, stale

    @patch('datasurface.cmd.platform.subprocess.run')
    def test_cloneToCache_success(self, mock_run):
        """Test successful clone to cache"""
        # Mock successful git clone
        mock_run.return_value = MagicMock(returncode=0)

        # Mock current time for consistent folder naming
        mock_time = 1234567890
        with patch('time.time', return_value=mock_time):
            # Mock os.path.exists to simulate .git directory creation
            with patch('os.path.exists') as mock_exists:
                # First call is for existing temp folder check, second is for .git validation
                mock_exists.side_effect = lambda path: path.endswith('.git')
                result = cloneToCache(self.cred_store, self.test_repo, self.cache_dir)

        expected_path = os.path.join(
            getCachedModelPath(self.cache_dir, self.test_repo),
            f"model-{mock_time:010d}"
        )
        self.assertEqual(result, expected_path)

        # Verify git clone was called with correct parameters
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        self.assertEqual(args[0], 'git')
        self.assertEqual(args[1], 'clone')
        self.assertEqual(args[2], '--branch')
        self.assertEqual(args[3], 'main')
        self.assertIn('github.com/testowner/testrepo.git', args[4])

    @patch('datasurface.cmd.platform.subprocess.run')
    def test_cloneToCache_failure(self, mock_run):
        """Test failed clone to cache"""
        # Mock failed git clone with CalledProcessError (which gets wrapped)
        from subprocess import CalledProcessError
        mock_run.side_effect = CalledProcessError(1, ['git', 'clone'], stderr="Authentication failed")

        with self.assertRaises(Exception) as context:
            cloneToCache(self.cred_store, self.test_repo, self.cache_dir)

        self.assertIn("Failed to clone repository", str(context.exception))

    def test_getCachedOrCloneModel_cache_hit(self):
        """Test getCachedOrCloneModel with cache hit"""
        # Create a fresh cache
        repo_cache_dir = getCachedModelPath(self.cache_dir, self.test_repo)
        os.makedirs(repo_cache_dir, exist_ok=True)

        current_time = int(time.time())
        cache_folder = os.path.join(repo_cache_dir, f"model-{current_time:010d}")
        os.makedirs(cache_folder, exist_ok=True)

        # Create eco.py to make it look like a valid model
        with open(os.path.join(cache_folder, "eco.py"), "w") as f:
            f.write("# Test ecosystem")

        with patch('datasurface.cmd.platform.isModelCacheStale', return_value=False):
            result = getCachedOrCloneModel(self.cred_store, self.test_repo, self.cache_dir, 5)
            self.assertEqual(result, cache_folder)

    @patch('datasurface.cmd.platform.cloneToCache')
    def test_getCachedOrCloneModel_cache_miss(self, mock_clone):
        """Test getCachedOrCloneModel with cache miss"""
        # Mock cloneToCache to return a path
        mock_path = "/mock/cache/path"
        mock_clone.return_value = mock_path

        result = getCachedOrCloneModel(self.cred_store, self.test_repo, self.cache_dir, 5)
        self.assertEqual(result, mock_path)
        mock_clone.assert_called_once_with(self.cred_store, self.test_repo, self.cache_dir)

    def test_cleanupOldCachedModels(self):
        """Test cleanup of old cached model versions"""
        # Create mock cached model folders
        repo_cache_dir = getCachedModelPath(self.cache_dir, self.test_repo)
        os.makedirs(repo_cache_dir, exist_ok=True)

        # Create 5 model folders with different timestamps
        timestamps = [1234567880, 1234567890, 1234567900, 1234567910, 1234567920]
        folders = []
        for ts in timestamps:
            folder = os.path.join(repo_cache_dir, f"model-{ts:010d}")
            os.makedirs(folder, exist_ok=True)
            with open(os.path.join(folder, "eco.py"), "w") as f:
                f.write("# Test ecosystem")
            folders.append(folder)

        # Keep only 3 newest
        cleanupOldCachedModels(self.cache_dir, self.test_repo, keep_count=3)

        # Check which folders still exist
        remaining = [f for f in folders if os.path.exists(f)]
        self.assertEqual(len(remaining), 3)

        # Should keep the 3 newest (highest timestamps)
        expected_remaining = [
            os.path.join(repo_cache_dir, "model-1234567900"),
            os.path.join(repo_cache_dir, "model-1234567910"),
            os.path.join(repo_cache_dir, "model-1234567920")
        ]
        for folder in expected_remaining:
            self.assertIn(folder, remaining)

    def test_cleanupOldCachedModels_no_cleanup_needed(self):
        """Test cleanup when no cleanup is needed"""
        # Create only 2 model folders
        repo_cache_dir = getCachedModelPath(self.cache_dir, self.test_repo)
        os.makedirs(repo_cache_dir, exist_ok=True)

        timestamps = [1234567890, 1234567900]
        folders = []
        for ts in timestamps:
            folder = os.path.join(repo_cache_dir, f"model-{ts:010d}")
            os.makedirs(folder, exist_ok=True)
            folders.append(folder)

        # Keep 3, but only have 2 - should not delete anything
        cleanupOldCachedModels(self.cache_dir, self.test_repo, keep_count=3)

        # All folders should still exist
        for folder in folders:
            self.assertTrue(os.path.exists(folder))

    @patch('datasurface.cmd.platform.getCachedOrCloneModel')
    @patch('datasurface.cmd.platform.loadEcosystemFromEcoModule')
    def test_getLatestModelAtTimestampedFolder_with_cache(self, mock_load, mock_get_cached):
        """Test getLatestModelAtTimestampedFolder using cache"""
        # Mock the cache returning a path
        mock_cache_path = "/mock/cache/model-1234567890"
        mock_get_cached.return_value = mock_cache_path

        # Mock loading ecosystem
        mock_eco = MagicMock()
        mock_tree = MagicMock()
        mock_tree.hasErrors.return_value = False
        mock_load.return_value = (mock_eco, mock_tree)

        eco, tree = getLatestModelAtTimestampedFolder(
            self.cred_store, self.test_repo, self.cache_dir,
            useCache=True, maxCacheAgeMinutes=5
        )

        self.assertEqual(eco, mock_eco)
        self.assertEqual(tree, mock_tree)

        # Verify cache was used
        mock_get_cached.assert_called_once_with(
            self.cred_store, self.test_repo, self.cache_dir, 5
        )
        mock_load.assert_called_once_with(mock_cache_path)

    def test_cache_path_safety(self):
        """Test that cache paths are safe and don't allow path traversal"""
        # Test with malicious repo names
        malicious_repo = GitHubRepository("../../../malicious", "main")
        path = getCachedModelPath(self.cache_dir, malicious_repo)

        # Should be sanitized - "/" gets replaced with "_"
        self.assertIn(".._.._.._malicious", path)
        self.assertTrue(path.startswith(self.cache_dir))

    def test_cache_concurrent_access_simulation(self):
        """Test cache behavior under simulated concurrent access"""
        # Create a cache folder
        repo_cache_dir = getCachedModelPath(self.cache_dir, self.test_repo)
        os.makedirs(repo_cache_dir, exist_ok=True)

        current_time = int(time.time())
        cache_folder = os.path.join(repo_cache_dir, f"model-{current_time:010d}")
        os.makedirs(cache_folder, exist_ok=True)

        # Simulate multiple processes trying to access the same cache
        for i in range(5):
            result = getLatestCachedModel(self.cache_dir, self.test_repo)
            self.assertEqual(result, cache_folder)

    def test_cache_edge_cases(self):
        """Test edge cases in cache functionality"""
        # Test with empty cache directory
        empty_cache = os.path.join(self.test_dir, "empty_cache")
        os.makedirs(empty_cache, exist_ok=True)

        result = getLatestCachedModel(empty_cache, self.test_repo)
        self.assertIsNone(result)

        # Test with cache directory that has non-model folders
        repo_cache_dir = getCachedModelPath(self.cache_dir, self.test_repo)
        os.makedirs(repo_cache_dir, exist_ok=True)

        # Create some non-model directories
        os.makedirs(os.path.join(repo_cache_dir, "not-a-model"))
        os.makedirs(os.path.join(repo_cache_dir, "also-not-model"))

        result = getLatestCachedModel(self.cache_dir, self.test_repo)
        self.assertIsNone(result)  # Should ignore non-model folders


if __name__ == '__main__':
    unittest.main()
