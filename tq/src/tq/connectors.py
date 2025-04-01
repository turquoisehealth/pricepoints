from pathlib import Path

import trino
from dotenv import dotenv_values


def get_trino_connection(
    env_file: Path | None = None,
) -> trino.dbapi.Connection:
    """
    Create a connection object for Turquoise Trino using an env file.

    :param env_file:
        Path to the .env file containing the connection parameters.
    :type env_file: Path

    :return:
        A Trino connection object for use with Pandas, Polars, etc.
    :rtype: trino.dbapi.Connection
    """

    env_file = env_file or Path(".env")
    config = dotenv_values(env_file)

    trino_conn = trino.dbapi.connect(
        host=config.get("TQ_TRINO_HOST", "trino"),
        port=int(str(config.get("TQ_TRINO_PORT", "443"))),
        catalog=config.get("TQ_TRINO_CATALOG", "hive"),
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(
            username=str(config.get("TQ_TRINO_USERNAME", "user")),
            password=str(config.get("TQ_TRINO_PASSWORD", "password")),
        ),
    )

    return trino_conn
