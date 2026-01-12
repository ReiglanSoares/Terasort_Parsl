import os
import argparse
import logging
import parsl
from config import gen_config
from apps import (
    filter_bucket,
    sort_bucket,
    verify_sorted_bucket,
)

# === PARÂMETROS ===
NUM_FILES = 256
NUM_BUCKETS = 256

INPUT_DIR = "inputs"
BUCKET_DIR = "buckets"
OUTPUT_DIR = "outputs"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        force=True
    )
    logging.getLogger("parsl").setLevel(logging.WARNING)
    logging.getLogger("parsl").propagate = False

def main(args):
    setup_logging()

    os.makedirs(BUCKET_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    input_files = [
        os.path.join(INPUT_DIR, f)
        for f in sorted(os.listdir(INPUT_DIR))
        if f.endswith(".bin")
    ]

    logging.info("========== TERASORT ==========")
    logging.info(f"Arquivos de entrada : {len(input_files)}")
    logging.info(f"Buckets             : {NUM_BUCKETS}")
    logging.info("=================================================")

    ranges = []
    for i in range(NUM_BUCKETS):
        min_key = bytes([i]) + b"\x00" * 9
        if i < NUM_BUCKETS - 1:
            max_key = bytes([i + 1]) + b"\x00" * 9
        else:
            max_key = b"\xff" * 10
        ranges.append((min_key, max_key))

    logging.info("[MAIN] Filtrando buckets...")

    filter_futures = []
    for bid, (min_k, max_k) in enumerate(ranges):
        bdir = os.path.join(BUCKET_DIR, f"bucket_{bid}")
        for f in input_files:
            filter_futures.append(
                filter_bucket(f, bdir, min_k, max_k)
            )

    for fut in filter_futures:
        fut.result()
    logging.info("[MAIN] Ordenando buckets...")

    sort_futures = []
    sorted_outputs = []

    for bid in range(NUM_BUCKETS):
        bdir = os.path.join(BUCKET_DIR, f"bucket_{bid}")
        out = os.path.join(OUTPUT_DIR, f"sorted_{bid}.bin")
        fut = sort_bucket(bdir, out)
        sort_futures.append(fut)
        sorted_outputs.append(out)

    for fut in sort_futures:
        fut.result()

    logging.info("[MAIN] Verificando buckets ordenados...")

    verify_futures = []
    for out in sorted_outputs:
        verify_futures.append(
            verify_sorted_bucket(out)
        )

    for fut in verify_futures:
        fut.result()

    logging.info("========== FIM TERASORT ==========")
  
    parsl.dfk().cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--onslurm", action="store_true")
    args = parser.parse_args()

    parsl.load(gen_config(slurm=args.onslurm))
    main(args)  
