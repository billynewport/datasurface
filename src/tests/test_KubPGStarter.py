"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from datasurface.cmd.platform import generatePlatformBootstrap


class Test_KubPGStarter(unittest.TestCase):
    def test_bootstrap(self):
        generatePlatformBootstrap("src/tests/kubpgtests", "src/tests/kubpgtests/base", "Test_DP")
