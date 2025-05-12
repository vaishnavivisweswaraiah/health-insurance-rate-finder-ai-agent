# Constants
CSV_FILE_PATH = "data/gold_Rate_PUF.csv"
VECTOR_STORE_DIR = "data/ChromaDB/vectorstore/vectorstore_db"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 50
BATCH_SIZE = 3000
MAX_WORKERS = 4  # Tune based on CPU
OFFSET_TRACK_FILE = "data/ChromaDB/checkpoint/vembedding_offset_checkpoint.txt"