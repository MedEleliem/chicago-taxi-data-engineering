import hashlib
import io
import json

import pytest
from botocore.exceptions import ClientError

from src import object_store as store


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.fail_upload = False

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body, metadata = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": hashlib.md5(body).hexdigest()}

    def put_object(self, Bucket, Key, Body, Metadata=None, **conditions):
        if self.fail_upload and Key.endswith("part.parquet"):
            raise RuntimeError("Simulated interrupted upload")
        if conditions.get("IfNoneMatch") and Key in self.objects:
            raise RuntimeError("Precondition failed")
        if conditions.get("IfMatch") and self.get_object(Bucket, Key)["ETag"] != conditions["IfMatch"]:
            raise RuntimeError("Concurrent publication")
        self.objects[Key] = (Body.read() if hasattr(Body, "read") else Body, Metadata or {})

    def head_object(self, Bucket, Key):
        body, metadata = self.objects[Key]
        return {"ContentLength": len(body), "Metadata": metadata}

    def download_file(self, Bucket, Key, Filename):
        from pathlib import Path
        Path(Filename).write_bytes(self.objects[Key][0])


def test_publication_download_and_failed_replacement(tmp_path, monkeypatch):
    s3 = MemoryS3()
    monkeypatch.setattr(store, "client", lambda: s3)
    output = tmp_path / "bronze/chicago_taxi/processing_date=2023-06-01"
    output.mkdir(parents=True)
    (output / "part.parquet").write_bytes(b"parquet-fixture")
    manifest = {"run_id": "BRZ_first", "processing_date": "2023-06-01", "output_path": str(output)}
    first = store.publish(manifest, "bronze", tmp_path)
    snapshot = store.download(first, tmp_path / "cache")
    assert (snapshot / first["output"] / "part.parquet").read_bytes() == b"parquet-fixture"
    s3.fail_upload = True
    with pytest.raises(RuntimeError, match="interrupted"):
        store.publish({**manifest, "run_id": "BRZ_second"}, "bronze", tmp_path)
    assert store.read_pointer(s3, "bronze", "2023-06-01")[0]["run_id"] == "BRZ_first"


def test_checksum_and_path_protection(tmp_path, monkeypatch):
    s3 = MemoryS3()
    monkeypatch.setattr(store, "client", lambda: s3)
    s3.objects["run/file"] = (b"corrupted", {})
    publication = {"run_id": "BRZ_bad", "prefix": "run/", "files": [
        {"path": "file", "size": 9, "sha256": "incorrect"}]}
    with pytest.raises(ValueError, match="checksum"):
        store.download(publication, tmp_path)
    assert not (tmp_path / "BRZ_bad").exists()
    with pytest.raises(ValueError, match="escapes"):
        store.contained(tmp_path, "../outside")


def test_restore_rebases_lineage_and_removes_old_parts(tmp_path, monkeypatch):
    s3 = MemoryS3()
    monkeypatch.setattr(store, "client", lambda: s3)
    source = tmp_path / "source"
    output = source / "bronze/chicago_taxi/processing_date=2023-06-01"
    output.mkdir(parents=True)
    (output / "part.parquet").write_bytes(b"new")
    report = source / "reports/bronze/BRZ_one"
    report.mkdir(parents=True)
    manifest = {"run_id": "BRZ_one", "processing_date": "2023-06-01", "output_path": str(output)}
    (report / "manifest.json").write_text(json.dumps(manifest))
    store.publish(manifest, "bronze", source)
    target = tmp_path / "target"
    old = target / output.relative_to(source)
    old.mkdir(parents=True)
    (old / "stale.parquet").write_bytes(b"old")
    store.restore("bronze", "2023-06-01", target)
    assert not (old / "stale.parquet").exists()
    restored = json.loads((target / "reports/bronze/BRZ_one/manifest.json").read_text())
    assert restored["output_path"] == str(old)
