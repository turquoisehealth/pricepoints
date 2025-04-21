import warnings
from pathlib import Path

import git


def get_project_root():
    """Get the git root directory of the research monorepo."""
    working_tree_dir = git.Repo(
        ".", search_parent_directories=True
    ).working_tree_dir

    if working_tree_dir is None:
        raise ValueError("Could not determine the git root directory.")

    return Path(working_tree_dir)


def get_env_file_path(env_file: Path | None = None) -> Path:
    """Get .env file path, using the first one found unless one is provided."""
    if env_file is None:
        cwd_env = Path(Path.cwd(), ".env")
        if cwd_env.exists():
            env_file = cwd_env
        else:
            env_file = Path(get_project_root(), ".env")

    if not env_file.exists():
        warnings.warn(f".env file not found at {env_file}. ")

    return env_file
