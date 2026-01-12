from parsl import python_app
import os

RECORD_SIZE = 100
KEY_SIZE = 10

@python_app
def filter_bucket(input_file, bucket_dir, min_key, max_key):
    os.makedirs(bucket_dir, exist_ok=True)
    out_file = os.path.join(bucket_dir, os.path.basename(input_file))

    with open(input_file, "rb") as fin, open(out_file, "wb") as fout:
        while True:
            rec = fin.read(RECORD_SIZE)
            if not rec:
                break
            key = rec[:KEY_SIZE]
            if min_key <= key < max_key:
                fout.write(rec)

    return out_file

@python_app
def sort_bucket(bucket_dir, output_file):
    records = []

    for fname in os.listdir(bucket_dir):
        path = os.path.join(bucket_dir, fname)
        with open(path, "rb") as f:
            while True:
                rec = f.read(RECORD_SIZE)
                if not rec:
                    break
                records.append(rec)

    records.sort(key=lambda r: r[:KEY_SIZE])

    with open(output_file, "wb") as out:
        for r in records:
            out.write(r)

    return output_file


@python_app
def verify_sorted_bucket(sorted_file):
    last_key = None

    with open(sorted_file, "rb") as f:
        while True:
            rec = f.read(RECORD_SIZE)
            if not rec:
                break

            key = rec[:KEY_SIZE]

            if last_key is not None and key < last_key:
                raise RuntimeError(
                    f"Arquivo NÃO ordenado corretamente: {sorted_file}"
                )

            last_key = key

    return True

@python_app
def finalize(sorted_files):
    return len(sorted_files)
