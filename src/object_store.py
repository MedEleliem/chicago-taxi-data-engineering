"""S3 publications: immutable run files, checksums, and one conditional latest pointer."""
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"],
                        region_name="us-east-1", config=Config(signature_version="s3v4",
                        s3={"addressing_style": "path"}, retries={"max_attempts": 3},
                        connect_timeout=5, read_timeout=60))


def bucket():
    return os.environ.get("S3_BUCKET", "chicago-taxi")


def pointer_key(layer, day):
    if layer not in ("bronze", "silver", "gold") or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise ValueError("Invalid publication identifier")
    return f"{layer}/processing_date={day}/latest.json"


def read_pointer(s3, layer, day):
    try:
        response = s3.get_object(Bucket=bucket(), Key=pointer_key(layer, day))
        with response["Body"] as stream:
            return json.loads(stream.read()), response["ETag"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None, None
        raise


def publish(manifest, layer, root):
    root = Path(root).resolve()
    s3 = client()
    day, run_id = manifest["processing_date"], manifest["run_id"]
    _, previous_etag = read_pointer(s3, layer, day)
    directories = [Path(manifest["output_path"])]
    if layer == "silver":
        directories.append(Path(manifest["quarantine_path"]))
    lineage = [(layer, run_id)]
    if layer in ("silver", "gold"):
        lineage.append(("bronze" if layer == "silver" else "silver", manifest["source_run_id"]))
    if layer == "gold":
        lineage.append(("bronze", manifest["bronze_run_id"]))
    directories += [root / "reports" / name / identifier for name, identifier in lineage]
    files = {file.resolve() for folder in directories for file in folder.rglob("*") if file.is_file()}
    for _, identifier in lineage:
        for file in (root / "reports/audit" / f"{identifier}.json", root / "logs" / f"{identifier}.log"):
            if file.exists():
                files.add(file.resolve())
    prefix = f"{layer}/processing_date={day}/runs/{run_id}/"
    inventory = []
    for file in sorted(files):
        relative = file.relative_to(root).as_posix()
        with file.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        with file.open("rb") as stream:
            s3.put_object(Bucket=bucket(), Key=prefix + relative, Body=stream,
                          Metadata={"sha256": digest}, IfNoneMatch="*")
        head = s3.head_object(Bucket=bucket(), Key=prefix + relative)
        if head["ContentLength"] != file.stat().st_size or head["Metadata"].get("sha256") != digest:
            raise ValueError("S3 upload verification failed")
        inventory.append({"path": relative, "size": file.stat().st_size, "sha256": digest})
    publication = {"layer": layer, "processing_date": day, "run_id": run_id,
                   "source_run_id": manifest.get("source_run_id"), "prefix": prefix,
                   "original_root": str(root), "files": inventory,
                   "output": Path(manifest["output_path"]).resolve().relative_to(root).as_posix()}
    if layer == "silver":
        publication["quarantine"] = Path(manifest["quarantine_path"]).resolve().relative_to(root).as_posix()
    body = json.dumps(publication).encode()
    s3.put_object(Bucket=bucket(), Key=prefix + "publication.json", Body=body, IfNoneMatch="*")
    condition = {"IfMatch": previous_etag} if previous_etag else {"IfNoneMatch": "*"}
    s3.put_object(Bucket=bucket(), Key=pointer_key(layer, day), Body=body, **condition)
    return publication


def contained(root, relative):
    path = (Path(root) / relative).resolve()
    if not path.is_relative_to(Path(root).resolve()) or path == Path(root).resolve():
        raise ValueError("Object path escapes snapshot directory")
    return path


def download(publication, cache_root):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", publication["run_id"]):
        raise ValueError("Invalid run_id")
    final = Path(cache_root).resolve() / publication["run_id"]
    if (final / "_download_complete.json").exists():
        return final
    staging = final.with_name(final.name + "-" + uuid4().hex)
    staging.mkdir(parents=True)
    s3 = client()
    try:
        for item in publication["files"]:
            target = contained(staging, item["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket(), publication["prefix"] + item["path"], str(target))
            with target.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if target.stat().st_size != item["size"] or digest != item["sha256"]:
                raise ValueError("S3 download checksum mismatch")
        (staging / "_download_complete.json").write_text(json.dumps(publication), encoding="utf-8")
        staging.rename(final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return final


def restore(layer, day, root):
    from src.common import promote
    root = Path(root).resolve()
    publication, _ = read_pointer(client(), layer, day)
    if publication is None:
        raise FileNotFoundError(f"No S3 publication: {layer} {day}")
    snapshot = download(publication, root / "object-cache")
    for key in ("output", "quarantine"):
        if key in publication:
            target = contained(root, publication[key])
            staging = root / "_restore" / uuid4().hex
            shutil.copytree(contained(snapshot, publication[key]), staging)
            promote(staging, target)
    for folder in ("reports", "logs"):
        if (snapshot / folder).exists():
            shutil.copytree(snapshot / folder, root / folder, dirs_exist_ok=True)
    # Local workspaces can differ; only metadata paths are rebased, never business values.
    for item in publication["files"]:
        if Path(item["path"]).name in ("manifest.json", "_manifest.json"):
            path = contained(root, item["path"])
            payload = json.loads(path.read_text(encoding="utf-8"))
            for key in ("input_path", "output_path", "quarantine_path"):
                value = payload.get(key)
                old = publication["original_root"]
                if value and value.startswith(old + os.sep):
                    payload[key] = str(root / value[len(old) + 1:])
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return publication


def gold_publications(cache_root):
    s3 = client()
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket(), Prefix="gold/"):
        for item in page.get("Contents", []):
            if re.fullmatch(r"gold/processing_date=\d{4}-\d{2}-\d{2}/latest.json", item["Key"]):
                day = item["Key"].split("/")[1].split("=")[1]
                publication, _ = read_pointer(s3, "gold", day)
                snapshot = download(publication, cache_root)
                path = contained(snapshot, publication["output"]) / "_manifest.json"
                manifest = json.loads(path.read_text(encoding="utf-8"))
                validation = json.loads((path.parent / "_validation.json").read_text(encoding="utf-8"))
                if manifest["run_id"] != publication["run_id"] or not validation.get("passed"):
                    raise ValueError("Invalid Gold object publication")
                result.append((path, manifest))
    return sorted(result, key=lambda item: item[1]["processing_date"])
