import sys
import os
# Add the root directory to the Python path
sys.path.append(os.getcwd())
from src.utils import dataframe_utils
from langchain_community.tools import DuckDuckGoSearchRun
from langchain.agents import Tool
from langchain.tools import StructuredTool
from typing import Optional
from pydantic import BaseModel
import traceback
from langchain_huggingface import HuggingFaceEmbeddings
from src.backend.vectorstore_builder import load_vectorstore
from src.backend.constants import VECTOR_STORE_DIR

 # : Embedding model
embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
class RateQuery(BaseModel):
    age: Optional[int] = None
    state: Optional[str] = None
    tobacco: Optional[str] = None

def read_rate_csv(**kwargs) -> str:
    try:
        print("[DEBUG] Received kwargs:", kwargs)
        # Construct Pydantic model from kwargs
        query =RateQuery(**kwargs)
        filename = 'Rate_PUF.csv'
        df= dataframe_utils.read_data_spark(file_path=f"data/{filename}",file_format="csv",header=True,inferSchema=True)
        df=df.filter(df["Age"].isNotNull())
        if query.age:
            df=df.filter(df['Age'] == query.age)
        if query.state:
            df=df.filter(df['StateCode'] == query.state)
        if query.tobacco:
            df=df.filter(df['Tobacco']== query.tobacco)

        return df.select("*").limit(10).toPandas().to_string(index=False)
    
    except Exception as e:
        print("[ERROR] Tool Exception:\n", traceback.format_exc())
        return f" exception occured while reading file {e}"
    

def search_vectorstore(**kwargs) -> str:
    try:
        print("[DEBUG] Received kwargs:", kwargs)
        # Construct Pydantic model from kwargs
        print(kwargs)
        query =RateQuery(**kwargs)
        

        # Load the Chroma vector store
        db = load_vectorstore(VECTOR_STORE_DIR,embedding=embedding)

        # Build metadata filter
        metadata_filter = {}
        if query.state is not None:
            metadata_filter["state"] = query.state
        
        # Perform similarity search with metadata filter
        search_query = query.query or f"insurance rates for age {query.age}, state {query.state}, tobacco {query.tobacco}"
        results = db.similarity_search(search_query, k=10, filter=metadata_filter)

        if not results:
            return "No matching documents found."

        # Format the results
        output_lines = []
        for i, doc in enumerate(results, 1):
            metadata = doc.metadata
            content = doc.page_content
            output_lines.append(f"Result {i}:\nMetadata: {metadata}\nContent: {content}\n")

        return "\n".join(output_lines)

    except Exception as e:
        print("[ERROR] Tool Exception:\n", traceback.format_exc())
        return f"Exception occurred while searching vector store: {e}"


# Define the structured tool
vector_tool = StructuredTool.from_function(
    name='search_vectorstore',
    description="Use this tool to find health insurance rate info using age, state, tobacco use, and optional natural language query.",
    func=search_vectorstore,
    args_schema=RateQuery
)   

search = DuckDuckGoSearchRun()
search_tool = Tool(
    name="duck_duck_go_search",
    func=search.run,
    description="Search the web for information",
)
read_csv_tool = StructuredTool.from_function(
    name='read_rates_file',
    description="Look up rates, plan names, issuers based on couple options, primary and all details  using age, state, and tobacco ",
    func=read_rate_csv,
    args_schema=RateQuery
)
    

