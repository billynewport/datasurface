"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.cmd.platform import generatePlatformBootstrap
from datasurface.md.governance import Ecosystem


class Test_KubPGStarter(unittest.TestCase):
    def test_bootstrap(self):
        eco: Ecosystem = generatePlatformBootstrap("src/tests/kubpgtests", "src/tests/kubpgtests/base", "Test_DP")
