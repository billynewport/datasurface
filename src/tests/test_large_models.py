"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md import Ecosystem
import os
from tests.model_large.eco import createEcosystem
import time


class TestLargeModels(unittest.TestCase):
    def test_load_large_model_10(self):
        # Set env var MAX_INDEX to 10
        os.environ["MAX_INDEX"] = "1000"
        # Time and load the model
        start_time = time.time()
        ecosys: Ecosystem = createEcosystem()
        end_time = time.time()
        print(f"Time taken to load the model: {end_time - start_time} seconds")
