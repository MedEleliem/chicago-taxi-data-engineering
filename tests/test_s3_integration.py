"""Opt-in checks against the real local S3 server, in an isolated temporary bucket."""
import os
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from src import object_store as store


@pytest.mark.skipif(os.getenv("CHICAGO_S3_TESTS") != "1", reason="Requires the local Compose object store")
def test_real_s3_publication_and_preconditions(tmp_path, monkeypatch):
    name = "chicago-test-" + uuid4().hex
    monkeypatch.setenv("S3_BUCKET", name)
    s3 = store.client()
    s3.create_bucket(Bucket=name)
    try:
        directory = tmp_path / "bronze/chicago_taxi/processing_date=2023-06-01"
        directory.mkdir(parents=True)
        (directory / "part.parquet").write_bytes(b"storage test fixture, not business data")
        manifest = {"run_id": "BRZ_first", "processing_date": "2023-06-01", "output_path": str(directory)}
        first = store.publish(manifest, "bronze", tmp_path)
        before, etag = store.read_pointer(s3, "bronze", "2023-06-01")
        assert before["run_id"] == "BRZ_first"
        with pytest.raises(ClientError):
            s3.put_object(Bucket=name, Key=store.pointer_key("bronze", "2023-06-01"),
                          Body=b"invalid", IfNoneMatch="*")
        second = store.publish({**manifest, "run_id": "BRZ_second"}, "bronze", tmp_path)
        assert store.read_pointer(s3, "bronze", "2023-06-01")[0]["run_id"] == "BRZ_second"
        with pytest.raises(ClientError):
            s3.put_object(Bucket=name, Key=store.pointer_key("bronze", "2023-06-01"), Body=b"stale", IfMatch=etag)
        snapshot = store.download(second, tmp_path / "cache")
        assert (snapshot / second["output"] / "part.parquet").read_bytes() == (directory / "part.parquet").read_bytes()
        assert s3.head_object(Bucket=name, Key=first["prefix"] + "publication.json")["ContentLength"] > 0
    finally:
        # Only the freshly-created test bucket is removed, never the application bucket.
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name):
            keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=name, Delete={"Objects": keys})
        s3.delete_bucket(Bucket=name)
