"""Subida en streaming a la zona landing (S3 / MinIO), sin escribir en disco."""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date

import boto3
from botocore.config import Config

from pipelines.ingest.settings import Settings

logger = logging.getLogger(__name__)

PART_SIZE = 8 * 1024 * 1024  # 8 MB por parte del multipart
_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class UploadResult:
    """Resultado de una subida en streaming."""

    key: str
    sha256: str
    size_bytes: int


def sanitize_filename(name: str, fallback: str = "archivo") -> str:
    """Normaliza a ASCII y deja solo caracteres seguros para una key de S3."""
    decoded = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    cleaned = _SAFE_CHARS.sub("-", decoded).strip("-._")
    return cleaned.lower() or fallback


def stable_url_id(url: str, length: int = 16) -> str:
    """Id corto y estable derivado de la URL (para fuentes sin id propio)."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:length]


def build_key(
    landing_prefix: str,
    resource_id: str,
    filename: str,
    ingest_date: date,
) -> str:
    """`{prefix}/resource_id=.../ingest_date=YYYY-MM-DD/{archivo}`."""
    return (
        f"{landing_prefix.strip('/')}"
        f"/resource_id={resource_id}"
        f"/ingest_date={ingest_date.isoformat()}"
        f"/{sanitize_filename(filename)}"
    )


def _rebuffer(chunks: Iterable[bytes], part_size: int) -> Iterator[bytes]:
    """Reagrupa chunks arbitrarios en bloques de `part_size` (el ultimo puede ser menor)."""
    buffer = bytearray()
    for chunk in chunks:
        if not chunk:
            continue
        buffer.extend(chunk)
        while len(buffer) >= part_size:
            yield bytes(buffer[:part_size])
            del buffer[:part_size]
    if buffer:
        yield bytes(buffer)


class LandingStorage:
    """Cliente S3 apuntado al bucket landing."""

    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region: str,
        bucket: str,
        client: object | None = None,
        part_size: int = PART_SIZE,
    ) -> None:
        self.bucket = bucket
        self.part_size = part_size
        self.client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    @classmethod
    def from_settings(cls, settings: Settings) -> LandingStorage:
        """Construye el cliente desde la configuracion del repo."""
        return cls(
            endpoint_url=settings.s3_endpoint_url,
            access_key_id=settings.s3_access_key_id,
            secret_access_key=settings.s3_secret_access_key,
            region=settings.s3_region,
            bucket=settings.s3_landing_bucket,
        )

    def upload_stream(self, key: str, chunks: Iterable[bytes]) -> UploadResult:
        """Sube un iterador de chunks con multipart y calcula sha256 al vuelo."""
        digest = hashlib.sha256()
        total = 0
        upload_id = self.client.create_multipart_upload(Bucket=self.bucket, Key=key)["UploadId"]
        parts: list[dict[str, object]] = []
        try:
            for number, part in enumerate(_rebuffer(chunks, self.part_size), start=1):
                digest.update(part)
                total += len(part)
                response = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    PartNumber=number,
                    UploadId=upload_id,
                    Body=part,
                )
                parts.append({"ETag": response["ETag"], "PartNumber": number})
                logger.debug("part subida key=%s numero=%d bytes=%d", key, number, len(part))
            if not parts:
                # S3 no acepta completar un multipart sin partes: objeto vacio por put_object.
                self.client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
                self.client.put_object(Bucket=self.bucket, Key=key, Body=b"")
            else:
                self.client.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
        except Exception:
            self.client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
            raise
        logger.info("landing key=%s bytes=%d partes=%d", key, total, len(parts))
        return UploadResult(key=key, sha256=digest.hexdigest(), size_bytes=total)

    def object_size(self, key: str) -> int | None:
        """Tamaño del objeto en landing, o None si no existe."""
        try:
            return int(self.client.head_object(Bucket=self.bucket, Key=key)["ContentLength"])
        except Exception:
            return None
