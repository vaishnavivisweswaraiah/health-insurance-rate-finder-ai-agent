from dotenv import load_dotenv
import os
import json
from langchain_openai import ChatOpenAI
from langchain.agents.agent_types import AgentType
from langchain.agents import initialize_agent
from tools import search_tool,vector_tool,read_csv_tool
from langchain.memory import ConversationBufferMemory
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# LangChain Agent Setup
load_dotenv()

llm= ChatOpenAI(temperature =0,openai_api_key=os.getenv('OPENAI_API_KEY'))

tools =[vector_tool,search_tool,read_csv_tool]

memory = ConversationBufferMemory(
    memory_key="chat_history",
    return_messages=True
)

agent_executor= initialize_agent(
    tools=tools,
    llm=llm,
    agent=AgentType.OPENAI_FUNCTIONS,
    verbose=False,
    memory=memory,
    agent_kwargs={"system_message" :"""
You are an intelligent health insurance assistant chatbot.
Use the following chat history to remember facts the user tells you (e.g. age, income, coverage needs):{chat_history}

Always reference prior information before making assumptions.

If the user has previously told you their age, use that instead of assuming

Your task:
- In user query first to look up rates and all details in the returned values using age, state, and tobacco use.
- Ask follow-up questions if any required information (age, tobacco use, state) or other things are missing.
- If data for user specified year is not available, use the 'search' tool to get latest or required details based on user query from the web
- Tobacco use values are in like 'No Preference', 'Tobacco User/Non-Tobacco User','NULL'  and state are present as state code like Ohio as 'OH' 
- Validate if response is in align with the query , if not , perform the reasoning again with additional search tools
- Output should contains all details user asked in the query also include additional plan details aswell.
"""}
)

def autonomous_agent(query: str) -> str:
    return agent_executor.invoke({"input": query},return_only_outputs=False)

if __name__ == "__main__":
    while True:
        query = input("Ask your insurance question (or 'exit'): ")
        if query.lower() == "exit":
            break
        try:
            result = autonomous_agent(query)
            print("\nResponse:\n", result['output'])
        except Exception as e:
            result = f"Agent Error: {e}"
            print("\nResponse:\n", result)
            break
            
        

