
# 🛡️ Health Insurance Bot

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![LangChain](https://img.shields.io/badge/LangChain-%F0%9F%A7%A1-lightgreen)](https://www.langchain.com/)
[![OpenAI](https://img.shields.io/badge/OpenAI-API-4B0082?logo=openai&logoColor=white)](https://platform.openai.com/)
[![PySpark](https://img.shields.io/badge/Spark-PySpark-orange?logo=apache-spark)](https://spark.apache.org/)
[![VS Code](https://img.shields.io/badge/Built%20with-VS%20Code-blue?logo=visual-studio-code)](https://code.visualstudio.com/)


# 🛡️ Health Insurance Bot

This project is an intelligent health insurance assistant chatbot powered by **LangChain**, **OpenAI**, and **PySpark**. It can retrieve the latest insurance rates based on age, state, and tobacco use, and automatically fall back to a web search if local data isn’t sufficient.

---

## 📌 Features

- 💬 Natural language interaction for insurance rate queries
- 🧠 AI-powered agent with LangChain + OpenAI
- 📊 Fast local filtering using PySpark
- 🌐 Web search fallback using DuckDuckGo
- 🧾 Structured data queries via custom tools
- ⚙️ Custom Java setup to support Spark

---

## 🧱 Project Structure

```
health-insurance-bot/
├── backend/
│   ├── agent_core.py          # Main chatbot logic
├   ├── Constants.py     
│   ├── tools.py               # LangChain tools with Spark + search
│   ├── hib.env                # Local environment variables
    ├── vectorstore_builder.py # RAG approach    
│
├── data/
│   └── Rate_PUF.csv           # Insurance rate data file
├   ├── Bronze_Rate_PUF.csv     
├   ├── silver_Rate_PUF.csv     
├   ├── gold_Rate_PUF.csv     
│
├── sparkJava/
│   └── jdk-11.0.26+4/...      # Java JDK (downloaded manually)
│
├── utils/
│   └── dataframe_utils.py     # PySpark data reading utils
├   ├── request_utils.py     
│
├── run.sh                     # Launch script: sets Java + opens VS Code
├── README.md
├── requirements.txt
├── .gitignore
```

---

## 🚀 Setup Instructions

### 1. 📥 Download & Place Java

Download **Java 11** (e.g. from [Adoptium](https://adoptium.net)) and extract it to:

```
sparkJava/jdk-11.0.26+4/
```

> ⚠️ **Do not push this folder** to Git — it's ignored via `.gitignore`.

---

### 2. 🔧 Set Java Paths

Update both `run.sh` and `tools.py` to point to your local JDK path:

```bash
export JAVA_HOME="$PWD/sparkJava/jdk-11.0.26+4/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

---

### 3. ▶️ Launch with VS Code

Run the setup and launch script:

```bash
./run.sh
```

This configures the environment and opens the project in VS Code.

---

### 4. 📊 Data Ingestion via PySpark

- The rate dataset is located in `data/Rate_PUF.csv`.
- Ingestion and filtering are handled via PySpark in `tools.py` and `dataframe_utils.py`.

> Filterable fields include:
> - `Age`
> - `StateCode` (e.g. "OH" for Ohio)
> - `Tobacco` (e.g. "Tobacco User", "Non-Tobacco User", etc.)

---

### 5. 🤖 Running the Bot

To start the chatbot:

```bash
python backend/agent_core.py
```

Example queries:

```
What are the rates for a 35-year-old non-tobacco user in OH?
List gold-level plans for a 50-year-old in CA.
```

If data is missing, the agent will search the web automatically using DuckDuckGo.

---

## 🔐 Environment Setup

Create a `.env` file (e.g. `hib.env`) with the following content:

```
OPENAI_API_KEY=your-openai-api-key-here
```

---

## 📦 Install Dependencies

Make sure you're using Python 3.9+.

Install required packages:

```bash
pip install -r requirements.txt
```

---

## 🛑 .gitignore Highlights

```gitignore
# Ignore downloaded JDK and generated Spark/IDE files
sparkJava/
spark-warehouse/
__pycache__/
*.pyc
.vscode/
.idea/
```

---

## 🙋‍♀️ Need Help?

Feel free to open an issue, submit a PR, or reach out for improvements and contributions!

---

