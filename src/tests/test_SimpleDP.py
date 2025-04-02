"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.md import Ecosystem, GitHubRepository, PlainTextDocumentation
from datasurface.platforms.simpledp import SimpleDataPlatform


class TestZero(unittest.TestCase):
    def test_zero(self):
        self.assertEqual(1, 1)

    def createEcosystem(self):
        eco: Ecosystem = Ecosystem(
            "TestZero",
            GitHubRepository("billynewport/test_zero", "main"),
            SimpleDataPlatform(
                "Simple",
                PlainTextDocumentation("Test Docs"),
                "stagingBucketName",
                "dataBucketName",
                "http://minio.local:8080",
                "dockerSecretFile")
        )
        return eco
