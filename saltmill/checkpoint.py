from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Optional

from saltmill.exceptions import CheckpointError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("saltmill")

_META_DIR = "_saltmill_meta"


class CheckpointManager:
    def __init__(self, spark: SparkSession, checkpoint_path: str) -> None:
        self._spark = spark
        self._checkpoint_path = checkpoint_path.rstrip("/")
        self._meta_path = f"{self._checkpoint_path}/{_META_DIR}"

    def setup(self) -> None:
        try:
            self._spark.sparkContext.setCheckpointDir(self._checkpoint_path)
            log.info("[saltmill] checkpoint dir: %s", self._checkpoint_path)
        except Exception as exc:
            raise CheckpointError(f"Failed to set checkpoint dir: {exc}") from exc

    def checkpoint_df(self, df: DataFrame, stage_name: str) -> DataFrame:
        log.info("[saltmill] checkpointing stage=%s", stage_name)
        try:
            checkpointed = df.checkpoint(eager=True)
            self.mark_stage_complete(stage_name)
            return checkpointed
        except Exception as exc:
            raise CheckpointError(
                f"Checkpoint failed at stage {stage_name!r}: {exc}"
            ) from exc

    def save_metadata(self, key: str, value: Any) -> None:
        path = f"{self._meta_path}/{key}.json"
        try:
            self._write_file(path, json.dumps(value))
        except Exception as exc:
            log.warning("[saltmill] Could not save metadata key=%s: %s", key, exc)

    def load_metadata(self, key: str) -> Optional[Any]:
        path = f"{self._meta_path}/{key}.json"
        try:
            raw = self._read_file(path)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def is_stage_complete(self, stage_name: str) -> bool:
        return self._file_exists(f"{self._meta_path}/{stage_name}.done")

    def mark_stage_complete(self, stage_name: str) -> None:
        self._write_file(f"{self._meta_path}/{stage_name}.done", "done")

    def _hadoop_fs(self, path_str: str):
        jvm = self._spark._jvm  # type: ignore[attr-defined]
        sc = self._spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        path_obj = jvm.org.apache.hadoop.fs.Path(path_str)
        return path_obj, path_obj.getFileSystem(conf)

    def _file_exists(self, path_str: str) -> bool:
        try:
            path_obj, fs = self._hadoop_fs(path_str)
            return bool(fs.exists(path_obj))
        except Exception:
            return False

    def _write_file(self, path_str: str, content: str) -> None:
        try:
            jvm = self._spark._jvm  # type: ignore[attr-defined]
            path_obj, fs = self._hadoop_fs(path_str)
            fs.mkdirs(jvm.org.apache.hadoop.fs.Path(path_str).getParent())
            out = fs.create(path_obj, True)
            out.write(content.encode("utf-8"))
            out.close()
        except Exception as exc:
            log.debug("[saltmill] _write_file failed for %s: %s", path_str, exc)

    def _read_file(self, path_str: str) -> Optional[str]:
        try:
            path_obj, fs = self._hadoop_fs(path_str)
            if not fs.exists(path_obj):
                return None
            inp = fs.open(path_obj)
            data = inp.readAllBytes()
            inp.close()
            return data.decode("utf-8")
        except Exception:
            return None
