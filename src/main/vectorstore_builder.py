import sys
import os
# Add the root directory to the Python path
sys.path.append(os.getcwd())
from src.utils.dataframe_utils import read_data_spark,read_last_processed_offset,write_last_processed_offset
from langchain.docstore.document import Document
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from itertools import islice
from typing import Optional
from src.main.constants import *

def process_batch(batch, chunk_size, chunk_overlap):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    raw_docs = [
        Document(
            page_content=row["full_text"],
            metadata={
                "age": row["Age"],
                "state": row["StateCode"],
                "tobacco": row["Tobacco"],
            }
        )
        for row in batch
    ]
    chunked_docs = text_splitter.split_documents(raw_docs)

    print(f"[INFO] Total chunks created after splitting: {len(chunked_docs)}")
    return chunked_docs

def build_vectorstore_from_csv(file_path: str) -> Chroma:
        df=read_data_spark(file_path=file_path,file_format='csv',header=True)
        df = df.orderBy("row_id")
        print(f"[INFO] Total raw rows (documents): {df.count()}")
        
        # : Embedding model
        embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
        db=None

        offset = read_last_processed_offset(OFFSET_TRACK_FILE)
        print(f"[INFO] Resuming from row offset: {offset}")

        # Read and process in batches
        rows = islice(df.select("full_text", "Age", "StateCode", "Tobacco").toLocalIterator(), offset, None)
        batch_num = 0
        while True:
            try:
                batch = list(islice(rows, BATCH_SIZE))
                if not batch:
                    break
                chunked_docs = process_batch(batch,CHUNK_SIZE,CHUNK_OVERLAP)

                if db is None:
                    db = Chroma.from_documents(
                                chunked_docs,
                                embedding,
                                persist_directory=VECTOR_STORE_DIR
                            )
                else:
                    db = load_vectorstore(VECTOR_STORE_DIR,embedding=embedding)
                    db.add_documents(chunked_docs)
            except Exception as e:
                print(f"error occured {e}")
            finally:
                offset += len(batch)
                write_last_processed_offset(offset,OFFSET_TRACK_FILE)
                batch_num += 1
                print(f"[INFO] Processed batch {batch_num}, total rows processed: {offset} with {len(chunked_docs)} chunks")
        
        print("[✅] Finished creating hybrid vector store with chunked documents.")
        return db

def load_vectorstore(save_path: str,embedding) -> Optional[Chroma]:
    if os.path.exists(save_path):
        return Chroma(persist_directory=save_path, embedding_function=embedding)
    else:
        return None

def build_vectorstore() -> Chroma:
        print("[INFO] Building fresh vectorstore from CSV...")
        db = build_vectorstore_from_csv(CSV_FILE_PATH)

if __name__ == "__main__":
    db = build_vectorstore()
    if db is not None:
        print("[INFO] Vector store ready to use!")
