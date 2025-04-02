from pathlib import Path

import git


def _get_project_root():
    """Get the git root directory of the research monorepo."""
    return Path(git.Repo(".", search_parent_directories=True).working_tree_dir)
