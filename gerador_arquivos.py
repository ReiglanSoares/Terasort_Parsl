import os
import random

# === PARÂMETROS ===
NUM_FILES = 256               
RECORDS_PER_FILE = 10_000_000 
OUTPUT_DIR = "inputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Gerando dados")

for i in range(NUM_FILES):
    path = os.path.join(OUTPUT_DIR, f"part-{i:05d}")
    random.seed(i)

    with open(path, "wb") as f:
        for _ in range(RECORDS_PER_FILE):
            key = random.randbytes(10)
            value = random.randbytes(90)
            f.write(key + value)

    print(f"Arquivo gerado: {path}")

print("Fim da geração dos dados.")
