#
# This file is part of exiom-native-fiber.
#
# Copyright (c) 2026 RoCEPool
# SPDX-License-Identifier: Apache-2.0

"""
Nightly tests: the longest simulations (8 and 16 nodes, networks with losses, wide links at scale).

They are skipped by default, so that the tests of every push stay short while each feature is
still covered at a small scale, and run every night by .github/workflows/nightly.yml:

    EXIOM_NIGHTLY=1 python3 -m pytest -v
"""

import os
import unittest

NIGHTLY = os.environ.get("EXIOM_NIGHTLY", "0") not in ["", "0"]

nightly = unittest.skipUnless(NIGHTLY, "Nightly test (EXIOM_NIGHTLY=1 to run).")
