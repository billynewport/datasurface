


import os
import sys
import unittest

from datasurface.handler.action import verifyPullRequest

class Test_ActionHandler(unittest.TestCase):
    def test_Step1(self):
        # Step 1: Create a new ActionHandler
        # Create a new ActionHandler

        os.environ.setdefault('HEAD_REPOSITORY', 'billynewport/test_step1')
        os.environ.setdefault("HEAD_BRANCH", 'EUmain')
        os.environ.setdefault("GITHUB_TOKEN", "Fake Token")

        headFolder : str = 'src/tests/actionHandlerResources/step2/head_EU'
        baseFolder : str = 'src/tests/actionHandlerResources/step1/base'

        cwd : str = os.getcwd()
        print(cwd)

        sys.argv = ["test_ActionHandler.py", baseFolder, headFolder]
        verifyPullRequest( )

