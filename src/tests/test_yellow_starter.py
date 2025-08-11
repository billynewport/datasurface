import unittest
from datasurface.md import Ecosystem, ValidationTree, DataPlatform, EcosystemPipelineGraph, PlatformPipelineGraph
from typing import Any, Optional
from datasurface.md.model_loader import loadEcosystemFromEcoModule


class TestEcosystem(unittest.TestCase):
    def test_createEcosystem(self):
        ecosys: Optional[Ecosystem]
        ecoTree: Optional[ValidationTree]
        ecosys, ecoTree = loadEcosystemFromEcoModule("src/tests/yellow_dp_tests/starter")
        self.assertIsNotNone(ecosys)
        self.assertIsNotNone(ecoTree)
        assert ecoTree is not None
        assert ecosys is not None
        if ecoTree.hasErrors():
            print("Ecosystem validation failed with errors:")
            ecoTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if ecoTree.hasWarnings():
                print("Note: There are some warnings:")
                ecoTree.printTree()
        vTree: ValidationTree = ecosys.lintAndHydrateCaches()
        if (vTree.hasErrors()):
            print("Ecosystem validation failed with errors:")
            vTree.printTree()
            raise Exception("Ecosystem validation failed")
        else:
            print("Ecosystem validated OK")
            if vTree.hasWarnings():
                print("Note: There are some warnings:")
                vTree.printTree()
        live_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("YellowLive")
        self.assertIsNotNone(live_dp)
        forensic_dp: DataPlatform[Any] = ecosys.getDataPlatformOrThrow("YellowForensic")
        self.assertIsNotNone(forensic_dp)
        graph: EcosystemPipelineGraph = ecosys.getGraph()
        self.assertIsNotNone(graph)
        live_root: Optional[PlatformPipelineGraph] = graph.roots.get(live_dp.name)
        self.assertIsNotNone(live_root)
        forensic_root: Optional[PlatformPipelineGraph] = graph.roots.get(forensic_dp.name)
        self.assertIsNotNone(forensic_root)


if __name__ == "__main__":
    unittest.main()
