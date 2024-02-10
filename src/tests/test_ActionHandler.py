


import os
import sys
import unittest

from datasurface.handler.action import verifyPullRequest
from datasurface.md.Lint import ValidationTree

class Test_ActionHandler(unittest.TestCase):
    def test_Step2(self):
        """This tries to start with a basic Ecosystem which defines 2 GZ and some resources. We then simulate a pull request to
        define the EU GZ and then the USA GZ. Finally we define the EU and USA teams and objects"""

        os.environ.setdefault("GITHUB_TOKEN", "Fake Token")

        headFolder : str = 'src/tests/actionHandlerResources/step2/head_EU'
        baseFolder : str = 'src/tests/actionHandlerResources/step1/base'

        os.environ.setdefault('HEAD_REPOSITORY', 'billynewport/test_step1')
        os.environ.setdefault("HEAD_BRANCH", 'EUmain')
        sys.argv = ["test_ActionHandler.py", baseFolder, headFolder]
        tree : ValidationTree = verifyPullRequest( )
        if(tree.hasErrors()):
            tree.printTree()
            self.fail("Tree has errors")

        # Now that the EU change is commited and the new baseline, lets see if the USA
        # pull request is valid
        baseFolder : str = 'src/tests/actionHandlerResources/step2/head_EU'
        headFolder : str = 'src/tests/actionHandlerResources/step2/head_USA'

        os.environ.setdefault('HEAD_REPOSITORY', 'billynewport/test_step1')
        os.environ.setdefault("HEAD_BRANCH", 'USAmain')
        sys.argv = ["test_ActionHandler.py", baseFolder, headFolder]
        tree = verifyPullRequest()
        if(tree.hasErrors()):
            tree.printTree()
            self.fail("Tree has errors")

        # Now step 2 head_USA is base, verify next step

        baseFolder : str = 'src/tests/actionHandlerResources/step2/head_USA'
        headFolder : str = 'src/tests/actionHandlerResources/step3'

        os.environ.setdefault('HEAD_REPOSITORY', 'billynewport/test_step1')
        os.environ.setdefault("HEAD_BRANCH", 'EUmain')
        sys.argv = ["test_ActionHandler.py", baseFolder, headFolder]
        tree = verifyPullRequest()
        if(tree.hasErrors()):
            tree.printTree()
            self.fail("Tree has errors")

        baseFolder : str = 'src/tests/actionHandlerResources/step3'
        headFolder : str = 'src/tests/actionHandlerResources/step4'

        os.environ.setdefault('HEAD_REPOSITORY', 'billynewport/test_step1')
        os.environ.setdefault("HEAD_BRANCH", 'USAmain')
        sys.argv = ["test_ActionHandler.py", baseFolder, headFolder]
        tree = verifyPullRequest()
        if(tree.hasErrors()):
            tree.printTree()
            self.fail("Tree has errors")



