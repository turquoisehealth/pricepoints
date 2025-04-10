from pathlib import Path

import pytest

from tq.utils import get_env_file_path


class TestGetEnvFilePath:
    def test_load_env_file_explicit_path(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.touch()

        result = get_env_file_path(env_file)
        assert result == env_file

    def test_load_env_file_cwd(self, tmp_path, monkeypatch):
        # Forcing the current working directory to be our temp path
        monkeypatch.setattr(Path, "cwd", lambda: tmp_path)

        env_file = tmp_path / ".env"
        env_file.touch()

        result = get_env_file_path()
        assert result == env_file

    def test_load_env_file_not_found(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "cwd", lambda: tmp_path)
        monkeypatch.setattr("tq.utils.get_project_root", lambda: tmp_path)

        with pytest.warns(UserWarning, match=".env file not found"):
            result = get_env_file_path()

        assert result == tmp_path / ".env"
