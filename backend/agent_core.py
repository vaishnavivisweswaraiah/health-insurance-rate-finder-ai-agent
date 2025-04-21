from dotenv import load_dotenv
import os
from langchain_openai import ChatOpenAI
from langchain.agents.agent_types import AgentType
from langchain.agents import initialize_agent
from langchain.prompts.chat import SystemMessagePromptTemplate,HumanMessagePromptTemplate,MessagesPlaceholder,ChatPromptTemplate
from tools import search_tool,read_tool

# LangChain Agent Setup
load_dotenv()

llm= ChatOpenAI(temperature =0,openai_api_key=os.getenv('OPENAI_API_KEY'))

# # This defines how the agent sees its task and the available tools
# chat_prompt = ChatPromptTemplate.from_messages([
#     SystemMessagePromptTemplate.from_template(
#          """
# You are an intelligent health insurance assistant chatbot.
# First, always attempt to look up rates using RateCSVReader. Use the user's age, tobacco use, state, or metal level to filter. Tobacco use is either Yes or No.
# If that fails, use WebSearch to retrieve updates about policies and rate changes.
# If you need more information from the user to proceed, ask follow-up questions."""),
#     MessagesPlaceholder(variable_name="chat_history"),
#     HumanMessagePromptTemplate.from_template("{query}"),
#     MessagesPlaceholder(variable_name="agent_scratchpad")
# ])

tools =[read_tool,search_tool]

agent_executor= initialize_agent(
    tools=tools,
    llm=llm,
    agent=AgentType.OPENAI_FUNCTIONS,
    verbose=True,
    agent_kwargs={"system_message" :"""
You are an intelligent health insurance assistant chatbot.

Your task:
- Always use the 'read_rates_file' tool first to look up rates and all details in the returned values using age, state, and tobacco use.
- If that fails, use the 'search' tool to get latest or required details based on user query from the web
- Ask follow-up questions if any required information (age, tobacco use, state) or other things are missing.
- Tobacco use values are in like 'No Preference', 'Tobacco User/Non-Tobacco User','NULL'  and state are present as state code like Ohio as 'OH' 
- When if data is not available from either of search, say you couldnot find in this method but found in other tool used.
- Output should contains all details user asked in the query
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
            print("\nResponse:\n", result)
        except Exception as e:
            result = f"Agent Error: {e}"
            print("\nResponse:\n", result)
            break
            
        

