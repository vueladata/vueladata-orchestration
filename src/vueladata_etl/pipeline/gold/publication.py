"""Publish a validated historical snapshot and refresh the existing Athena paths."""

import json
from urllib.parse import urlsplit
from uuid import uuid4

import boto3


class HistoricalGoldPublication:
    def __init__(self, gold_path, s3=None):
        location = urlsplit(gold_path)
        self.bucket = location.netloc
        self.gold_prefix = location.path.strip("/")
        parent, separator, _ = self.gold_prefix.rpartition("/")
        self.snapshots = f"{parent}{separator}_gold_snapshots"
        self.snapshot = f"{self.snapshots}/{uuid4().hex}"
        self.path = f"s3a://{self.bucket}/{self.snapshot}"
        self.s3 = s3 if s3 is not None else boto3.client("s3")

    def _keys(self, prefix):
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            yield from (entry["Key"] for entry in page.get("Contents", []))

    def publish(self, tables):
        tables = list(tables)
        plans = []
        for name in tables:
            source = f"{self.snapshot}/{name}/"
            target = f"{self.gold_prefix}/{name}/"
            relative_keys = {key[len(source):] for key in self._keys(source)}
            data = sorted(key for key in relative_keys if key.endswith(".parquet") and not key.startswith("_"))
            if "_SUCCESS" not in relative_keys or not data:
                raise RuntimeError(f"Gold snapshot is incomplete: {name}")
            plans.append((source, target, data))

        # Copy every new file before removing old data from the Athena paths.
        # Readers that require a consistent version can resolve the snapshot through latest.json.
        for source, target, data in plans:
            for relative_key in data:
                self.s3.copy(
                    {"Bucket": self.bucket, "Key": source + relative_key},
                    self.bucket, target + relative_key,
                )

        for _, target, data in plans:
            keep = {target + key for key in data}
            obsolete = [key for key in self._keys(target) if key not in keep]
            self._delete_obsolete(obsolete)
            self.s3.put_object(Bucket=self.bucket, Key=f"{target}_SUCCESS", Body=b"")
            print(f"[Gold] Published {target} with {len(data)} Parquet files", flush=True)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{self.snapshots}/latest.json",
            Body=json.dumps({"path": self.path, "tables": tables}).encode(),
            ContentType="application/json",
        )

    def _delete_obsolete(self, keys):
        for offset in range(0, len(keys), 1000):
            response = self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": [{"Key": key} for key in keys[offset:offset + 1000]],
                    "Quiet": True,
                },
            )
            if response.get("Errors"):
                raise RuntimeError(f"Could not remove obsolete Gold objects: {response['Errors']}")
