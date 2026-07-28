from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def normalize_snowflake_account(value: str) -> str:
    account = value.strip()
    account = account.removeprefix("https://").removeprefix("http://")
    account = account.removesuffix("/")
    return account.removesuffix(".snowflakecomputing.com")


def normalize_xactly_jdbc_url(value: str) -> str:
    url = value.strip()
    if url.startswith("xactly://"):
        return f"jdbc:{url}"
    return url


@dataclass(frozen=True)
class OpenAISettings:
    provider: str
    model: str
    api_key: str
    azure_api_key: str
    azure_endpoint: str
    azure_api_version: str
    azure_deployment: str

    @classmethod
    def from_env(cls) -> "OpenAISettings":
        return cls(
            provider=os.getenv("OPENAI_PROVIDER", "openai").strip().lower(),
            model=os.getenv("OPENAI_MODEL", "gpt-4.1").strip(),
            api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            azure_api_key=os.getenv("AZURE_OPENAI_API_KEY", "").strip(),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "").strip(),
            azure_api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21").strip(),
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip(),
        )


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str

    @classmethod
    def from_env(cls) -> "SnowflakeSettings":
        return cls(
            account=normalize_snowflake_account(os.getenv("SNOWFLAKE_ACCOUNT", "")),
            user=os.getenv("SNOWFLAKE_USER", "").strip(),
            password=os.getenv("SNOWFLAKE_PASSWORD", "").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "CS_BOT_WH").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE", "CUSTOMER_SUPPORT_BOT_LOGS").strip(),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "CHAT_DATA").strip(),
            role=os.getenv("SNOWFLAKE_ROLE", "").strip(),
        )

    @property
    def is_complete(self) -> bool:
        return all(
            [
                self.account,
                self.user,
                self.password,
                self.warehouse,
                self.database,
                self.schema,
            ]
        )


@dataclass(frozen=True)
class XactlyJdbcSettings:
    url: str
    user: str
    password: str
    jar_path: str
    driver_class: str
    pod_name: str
    java_home: str
    mode: str

    @classmethod
    def from_env(cls) -> "XactlyJdbcSettings":
        return cls(
            url=normalize_xactly_jdbc_url(
                os.getenv("XACTLY_JDBC_URL", "xactly://secure3.xactlycorp.com:443?useSSL=")
            ),
            user=os.getenv("XACTLY_JDBC_USER", "").strip(),
            password=os.getenv("XACTLY_JDBC_PASSWORD", "").strip(),
            jar_path=os.getenv(
                "XACTLY_JDBC_JAR_PATH",
                "drivers/xjdbc-2.2.3-RELEASE-jar-with-dependencies.jar",
            ).strip(),
            driver_class=os.getenv("XACTLY_JDBC_DRIVER_CLASS", "com.xactly.connect.jdbc.Driver").strip(),
            pod_name=os.getenv("XACTLY_JDBC_POD", "secure3").strip(),
            java_home=os.getenv(
                "XACTLY_JAVA_HOME",
                "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
            ).strip(),
            mode=os.getenv("XACTLY_JDBC_MODE", "subprocess").strip().lower(),
        )

    @property
    def is_complete(self) -> bool:
        return all(
            [
                self.url,
                self.user,
                self.password,
                self.jar_path,
                self.driver_class,
            ]
        )
