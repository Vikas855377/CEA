"""Generate client-specific Obero Monitoring packages from the approved template."""

from __future__ import annotations

import base64
import io
import re
import zipfile
from dataclasses import dataclass
from xml.etree import ElementTree


# New Package (3).spm, supplied on 2026-07-27. Keeping the compressed source
# intact avoids XML reformatting that can make Obero package imports brittle.
_TEMPLATE_B64 = """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"""

_SLUG = re.compile(r"^[a-z][a-z0-9_]{1,62}$")


@dataclass(frozen=True)
class PackageConfig:
    client_id: int
    client_name: str
    staging_prefix: str
    process_id: int = 143
    package_name: str = "Monitoring"

    def validate(self) -> None:
        if self.client_id < 1 or self.process_id < 1:
            raise ValueError("Client ID and Process ID must be positive integers")
        if not self.client_name.strip() or len(self.client_name.strip()) > 100:
            raise ValueError("Client name is required and must be at most 100 characters")
        if "'" in self.client_name:
            raise ValueError("Client name cannot contain a single quote")
        if not _SLUG.fullmatch(self.staging_prefix):
            raise ValueError("Staging prefix must start with a letter and use lowercase letters, numbers, or underscores")
        if not self.package_name.strip() or len(self.package_name.strip()) > 100:
            raise ValueError("Package name is required and must be at most 100 characters")


def _replace_sql(value: str, config: PackageConfig) -> str:
    value = re.sub(r"delta\.test_", f"delta.{config.staging_prefix}_", value, flags=re.I)
    value = re.sub(r"\b1(\s+AS\s+clientid\b)", rf"{config.client_id}\1", value, flags=re.I)
    value = re.sub(r"(\bclientid\s*=\s*)1\b", rf"\g<1>{config.client_id}", value, flags=re.I)
    value = re.sub(r"'Test'(\s+(?:AS\s+)?clientname\b)", lambda m: f"'{config.client_name}'{m.group(1)}", value, flags=re.I)
    return value


def generate_package(config: PackageConfig) -> tuple[bytes, str]:
    config.validate()
    source = io.BytesIO(base64.b64decode(_TEMPLATE_B64))
    output = io.BytesIO()
    with zipfile.ZipFile(source) as template, zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as package:
        process_xml = template.read("143.xml").decode("utf-8")
        root = ElementTree.fromstring(process_xml)
        root.set("Name", config.package_name.strip())
        tasks = root.find("Tasks")
        if tasks is None:
            raise ValueError("Template package does not contain workflow tasks")
        for task in tasks:
            for attribute in ("SourceSqlQuery", "DestinationTable"):
                if attribute in task.attrib:
                    task.set(attribute, _replace_sql(task.get(attribute, ""), config))
            # ConnectCommand SQL is stored as the TaskType child's tail, while
            # XactlyPush SQL is stored in attributes. Transform both forms.
            for node in task.iter():
                if node.text:
                    node.text = _replace_sql(node.text, config)
                if node.tail:
                    node.tail = _replace_sql(node.tail, config)
        xml_bytes = ElementTree.tostring(root, encoding="utf-8", xml_declaration=True)
        package.writestr(f"{config.process_id}.xml", xml_bytes)

        description = ElementTree.fromstring(template.read("app-description.spmx"))
        description.set("PackageName", config.package_name.strip())
        process = description.find("Process")
        if process is None:
            raise ValueError("Template package does not contain process metadata")
        process.set("Id", str(config.process_id))
        process.set("Name", config.package_name.strip())
        process.set("Path", "/WIP/Monitoring")
        package.writestr(
            "app-description.spmx",
            ElementTree.tostring(description, encoding="utf-8", xml_declaration=False),
        )
    filename = f"{config.staging_prefix}-monitoring.spm"
    return output.getvalue(), filename
