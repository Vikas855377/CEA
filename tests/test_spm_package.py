import io
import re
import unittest
import zipfile

from projects.ProcessRunAutomation.spm_package import PackageConfig, generate_package


class SpmPackageTests(unittest.TestCase):
    def test_generates_consistent_client_specific_package(self) -> None:
        payload, filename = generate_package(
            PackageConfig(4, "docusigntemp", "docusigntemp", 192135)
        )

        self.assertEqual(filename, "docusigntemp-monitoring.spm")
        with zipfile.ZipFile(io.BytesIO(payload)) as package:
            self.assertIsNone(package.testzip())
            self.assertEqual(
                set(package.namelist()), {"192135.xml", "app-description.spmx"}
            )
            process = package.read("192135.xml").decode("utf-8")
            metadata = package.read("app-description.spmx").decode("utf-8")

        self.assertNotRegex(process, r"(?i)clientid\s*=\s*1\b")
        self.assertNotRegex(process, r"(?i)delta\.test_")
        self.assertEqual(len(re.findall(r"(?i)clientid\s*=\s*4\b", process)), 15)
        self.assertEqual(len(re.findall(r"(?i)delta\.docusigntemp_", process)), 12)
        self.assertIn("'docusigntemp'", process)
        self.assertIn('Process Id="192135"', metadata)
        self.assertIn('Path="/WIP/Monitoring"', metadata)
        self.assertNotIn('Path="/CEA/WIP/Monitoring"', metadata)

    def test_rejects_unsafe_staging_prefix(self) -> None:
        with self.assertRaisesRegex(ValueError, "Staging prefix"):
            generate_package(PackageConfig(4, "DocuSign", "bad-prefix"))


if __name__ == "__main__":
    unittest.main()
