# RAG-to-SQL Query Generator

A project that leverages **Retrieval-Augmented Generation (RAG)** and large language models (LLMs) to convert natural language questions into structured SQL queries. Designed for seamless integration with databases, this tool simplifies querying data for non-technical users and enhances developer productivity.

---

## Features
- **Natural Language to SQL**: Transform plain English questions into executable SQL queries.
- **RAG-Powered Context**: Retrieve relevant database schema and context using embeddings and a vector database.
- **Multi-Dialect Support**: Generate SQL compatible with PostgreSQL.
- **Customizable Prompts**: Adapt templates to specific use cases or database schemas.
- **Query Validation**: Ensure syntactically correct SQL with built-in parsing and error handling.

---

## Installation

### Prerequisites
- Python 3.8+
- **UV** installed globally:
  ```bash
  pip install uv
  
  git clone https://github.com/sunil-thapa99/RAG-to-SQL.git
  cd RAG-to-SQL
  ```

### Configuration

1. Install the required Python packages:

   ```bash
   uv pip install -r requirements.txt
   ```

2. Create a `.env` file in the project directory and add the following environment variables:\
<em>Copy .env.example, and rename it to .env</em>
   ```plaintext
   DB_HOST=localhost
   DB_PORT=5433
   DB_NAME=rag_to_sql
   DB_USER=postgres
   DB_PASSWORD=1234
   MODEL_NAME=gpt-3.5-turbo  # or local model path
   VECTOR_DB_PATH=./vector_db
   ```


