**Demo-RAG: Retrieval-Augmented Generation (Educational Demo)**

**Project Overview**

Demo-RAG is an educational demonstration project that showcases how to build a
Retrieval-Augmented Generation (RAG) pipeline step-by-step using Python. The project
demonstrates scraping data, chunking text, generating embeddings, building a retriever, and finally
creating a chatbot powered by retrieved knowledge.

**What is RAG?**
Retrieval-Augmented Generation (RAG) enhances Large Language Models by retrieving relevant
context from external data sources before generating responses. Instead of relying solely on model
memory, RAG grounds responses in real data.

**Project Workflow**
• Data Collection – Scrape Wikipedia or other sources.
• Chunking – Break large text into smaller pieces.
• Embedding – Convert text chunks into vector representations.
• Ingestion – Store embeddings into a vector store.
• Retrieval – Fetch relevant chunks based on user query.
• Generation – Use LLM to generate contextual responses

**Project Structure**

1_scraping_wikipedia.py
2_chunking_embedding_ingestion.py
3_chatbot.py
example_chunking.py
example_embedding.py
example_retriever.

**How to Run**
python 1_scraping_wikipedia.py
python 2_chunking_embedding_ingestion.py
python 3_chatbot.py

**Key Features**
• Step-by-step RAG pipeline demonstration
• Embedding generation and storage
• Retriever implementation example
• Context-aware chatbot
• Educational and beginner-friendly structure
