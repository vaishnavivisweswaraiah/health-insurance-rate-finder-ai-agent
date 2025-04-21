import sys
import os
# Add the root directory to the Python path
sys.path.append(os.getcwd())
from utils import dataframe_utils
from langchain_community.tools import DuckDuckGoSearchRun
from langchain.agents import Tool
from langchain.tools import StructuredTool
from typing import Optional
from pydantic import BaseModel
import traceback
# Ensure JAVA_HOME is set so Spark can initialize correctly in a fresh process.
os.environ["JAVA_HOME"] = "/Users/vaishnavi/Desktop/Research/LLM/health-insurance-bot/sparkJava/jdk-11.0.26+4/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
from pyspark.sql import SparkSession
#Initialize Spark Session
spark = SparkSession.builder.appName("Health Insurance").getOrCreate()

# Define a Pydantic model for structured tool input
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
        df= dataframe_utils.read_data_spark(file_path=f"data/{filename}",file_format="csv",spark=spark,header=True,inferSchema=True)
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
        return f" execption occured while reading file {e}"
    
read_tool = StructuredTool.from_function(
    name='read_rates_file',
    description="Look up rates, plan names, issuers based on couple options, primary and all details  using age, state, and tobacco ",
    func=read_rate_csv,
    args_schema=RateQuery
)
search = DuckDuckGoSearchRun()
search_tool = Tool(
    name="duck_duck_go_search",
    func=search.run,
    description="Search the web for information",
)
    

