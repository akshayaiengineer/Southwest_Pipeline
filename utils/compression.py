import gzip
import shutil
from pathlib import Path

from utils.logger import get_logger


logger = get_logger("file-compressor")


def compress_file(
    input_file_path: str,
) -> str | None:

    input_path = Path(input_file_path)

    if not input_path.exists():
        logger.error(
            "Input file was not found | path=%s",
            input_path,
        )
        return None

    output_path = Path(f"{input_path}.gz")

    try:
        with input_path.open("rb") as source_file:
            with gzip.open(output_path, "wb") as compressed_file:
                shutil.copyfileobj(
                    source_file,
                    compressed_file,
                )

        logger.info(
            "File compressed successfully | input=%s | output=%s",
            input_path,
            output_path,
        )

        return str(output_path)

    except Exception:
        logger.exception(
            "File compression failed | input=%s",
            input_path,
        )
        return None