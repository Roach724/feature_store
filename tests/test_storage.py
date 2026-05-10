import os
import tempfile
import pytest
from feature_store.storage import LocalBackend, get_backend


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def backend():
    return LocalBackend()


class TestLocalBackend:
    def test_exists_true(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "test.txt")
        with open(path, "w") as f:
            f.write("hello")
        assert backend.exists(path) is True

    def test_exists_false(self, backend, tmp_dir):
        assert backend.exists(os.path.join(tmp_dir, "nope.txt")) is False

    def test_open_read(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "data.yaml")
        content = "key: value\n"
        with open(path, "w") as f:
            f.write(content)
        with backend.open(path, "r") as f:
            assert f.read() == content

    def test_open_write(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "out.yaml")
        with backend.open(path, "w") as f:
            f.write("hello")
        with open(path) as f:
            assert f.read() == "hello"

    def test_glob(self, backend, tmp_dir):
        for name in ["a.yaml", "b.yaml", "c.txt"]:
            with open(os.path.join(tmp_dir, name), "w") as f:
                f.write("x")
        results = backend.glob(os.path.join(tmp_dir, "*.yaml"))
        assert len(results) == 2
        assert all(p.endswith(".yaml") for p in results)

    def test_cp_file(self, backend, tmp_dir):
        src = os.path.join(tmp_dir, "src.txt")
        dst = os.path.join(tmp_dir, "dst.txt")
        with open(src, "w") as f:
            f.write("copied")
        backend.cp(src, dst)
        assert os.path.exists(dst)
        with open(dst) as f:
            assert f.read() == "copied"

    def test_rm_file(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "remove_me.txt")
        with open(path, "w") as f:
            f.write("x")
        backend.rm(path)
        assert not os.path.exists(path)

    def test_rm_dir_recursive(self, backend, tmp_dir):
        d = os.path.join(tmp_dir, "sub")
        os.makedirs(os.path.join(d, "nested"))
        with open(os.path.join(d, "f.txt"), "w") as f:
            f.write("x")
        backend.rm(d, recursive=True)
        assert not os.path.exists(d)

    def test_rm_nonexistent_no_error(self, backend, tmp_dir):
        backend.rm(os.path.join(tmp_dir, "nonexistent"))

    def test_cp_directory(self, backend, tmp_dir):
        src_dir = os.path.join(tmp_dir, "srcdir")
        dst_dir = os.path.join(tmp_dir, "dstdir")
        os.makedirs(src_dir)
        with open(os.path.join(src_dir, "f.txt"), "w") as f:
            f.write("data")
        backend.cp(src_dir, dst_dir)
        assert os.path.exists(os.path.join(dst_dir, "f.txt"))


class TestGetBackend:
    def test_local_backend_for_path(self):
        b = get_backend("/tmp/foo")
        assert isinstance(b, LocalBackend)

    def test_local_backend_for_relative(self):
        b = get_backend("./some/path")
        assert isinstance(b, LocalBackend)
