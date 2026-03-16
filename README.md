# VARA.ai

![Amazon Nova](https://img.shields.io/badge/Amazon%20Nova-Powered-FF9900?style=for-the-badge&logo=amazon)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python)
![MCP](https://img.shields.io/badge/MCP-Model%20Context%20Protocol-purple?style=for-the-badge)
![Multi-Agent](https://img.shields.io/badge/Multi--Agent-5%20Agents-orange?style=for-the-badge)
![Neo4j](https://img.shields.io/badge/Neo4j-Aura-008CC1?style=for-the-badge&logo=neo4j)

> **A multi-agent customer support platform built with Amazon Nova, Neo4j, and MCP that delivers verified refund decisions with grounded, explainable reasoning from a policy knowledge graph.**

### 🏆 Amazon Nova AI Hackathon Submission

---

## 🚀 Live Demo

| | |
|---|---|
| **Demo URL** | [https://staging.d1vug68j94viep.amplifyapp.com/](https://staging.d1vug68j94viep.amplifyapp.com/) |
| **Demo Video** | [YouTube Demo](#) <!-- Add your YouTube link here --> |

> **No login required.** The demo is publicly accessible.

---

## 📋 Problem Statement

Customer support teams spend **70%+ of their time** on repetitive refund/return requests. Each request requires:
- Reading customer emails with attachments
- Cross-referencing order databases
- Consulting return policy documents
- Making consistent decisions

**VARA.ai automates this entire workflow using Amazon Nova's advanced reasoning capabilities.**

---

## 💡 Solution Overview

VARA.ai is an end-to-end AI system that:

1. **Monitors Gmail** for incoming refund/return requests
2. **Classifies emails** using Amazon Nova's understanding capabilities
3. **Extracts order details** from email text and invoice attachments (Nova Vision)
4. **Verifies against database** using MCP-connected PostgreSQL
5. **Adjudicates requests** by traversing a policy knowledge graph
6. **Records decisions** with full reasoning transparency

---

## 🧪 Testing Instructions

### Demo URL
**[https://staging.d1vug68j94viep.amplifyapp.com/](https://staging.d1vug68j94viep.amplifyapp.com/)**

**No login required.** The demo is publicly accessible.

---

### Feature 1: Email Processing Pipeline

1. **Open the demo URL** — You'll land on the Email Pipeline page

2. **Select a scenario** from the "Select Demo Scenario" dropdown

3. **View the request** — Email content and attached invoice displayed on the left panel

4. **Click "Process Email Request"** to trigger the full AI pipeline

5. **Watch the pipeline** execute in real-time (~1 minute to complete):
   - Email Classification
   - Order Extraction (Nova Vision)
   - Database Verification
   - Policy Adjudication (Deep Reasoning)
   - Decision with Explanation

> **Note:** In production, this pipeline runs automatically when a customer sends an email to `vara.assist@gmail.com`. The demo uses pre-defined scenarios because processing a real email requires the corresponding order to exist in our database. This website serves as a prototype to demonstrate the fully automated end-to-end pipeline.

---

### Feature 2: Policy Knowledge Base

1. **Click "Policy Knowledge Base"** in the navigation bar

#### Option A: View Existing Graph
- Click **"Visualize Graph"** to view the pre-compiled knowledge graph (Best Buy return policy)
- **Interactive controls:**
  - Scroll to zoom
  - Drag to pan
  - Click nodes for details

#### Option B: Compile New Policy (20-25 min)
1. Upload any company's terms and conditions PDF
2. Full compilation takes ~20-25 minutes (based on document length), orchestrated entirely by Amazon Nova Pro

**The multi-agent system will automatically:**
```
📄 Parse PDF → Markdown (LlamaParse)
        ↓
🧠 Design graph schema (Ontology Agent)
        ↓
📤 Extract entities & relationships (Extraction Agent)
        ↓
🔍 Validate quality (Critic Agent)
        ↓
🔨 Build Neo4j graph (Builder Agent)
```

---

## 🤖 Multi-Agent System

### 5 Specialized Agents

| Agent | Role | Amazon Nova Feature |
|-------|------|---------------------|
| **Ontology Agent** | Designs knowledge graph schema from policy documents | Deep Reasoning |
| **Extraction Agent** | Extracts policy rules, conditions, and relationships | Content Extraction |
| **Critic Agent** | Validates extraction quality and suggests improvements | System Prompting |
| **Builder Agent** | Constructs Neo4j knowledge graph with Cypher queries | Structured Output |
| **Adjudicator Agent** | Makes refund decisions with full reasoning | Policy Navigation |

---

## 🔧 MCP (Model Context Protocol) Servers

VARA.ai uses **FastMCP** to create modular, tool-based AI capabilities:

### `db_verification_server` — Order Database Access
| Tool | Description |
|------|-------------|
| `list_orders_by_customer_email` | Fetch order history for a customer email |
| `find_order_by_invoice_number` | Lookup single order with full details |
| `find_order_by_order_invoice_id` | Alternative lookup by order_invoice_id |
| `list_order_items_by_order_invoice_id` | Get line items for an order |
| `verify_from_email_matches_customer` | Check if email exists in customers table |
| `get_customer_orders_with_items` | Deep fetch with order items |
| `select_order_id` | LLM-assisted order matching |
| `llm_find_orders` | Generate SQL from natural language |

### `doc_server` — Invoice Processing
| Tool | Description |
|------|-------------|
| `process_invoice` | Decode base64 PDF, parse text, and save to file |

### `defect_analyzer` — Product Defect Analysis
| Tool | Description |
|------|-------------|
| `analyze_defect_image` | Analyze product defect images using Nova Vision |

---

## 🔷 Core Services

### Neo4j Graph Engine
Policy knowledge graph operations for storing and querying return policies.

| Function | Description |
|----------|-------------|
| `check_neo4j_connection` | Test database connectivity |
| `get_graph_schema` | Retrieve node labels and relationships |
| `get_graph_statistics` | Node/relationship counts |
| `execute_cypher_query` | Run read-only Cypher queries |
| `execute_cypher_write` | Run write Cypher (CREATE, MERGE) |
| `execute_cypher_batch` | Bulk graph construction |
| `create_node` | Create/merge a node with properties |
| `create_relationship` | Create relationship between nodes |
| `create_schema_constraints` | Set up indexes and constraints |
| `clear_graph` | Delete all data (destructive) |
| `validate_graph_integrity` | Check for missing citations, orphans |
| `sample_graph_data` | Get sample nodes for verification |

### Policy Engine
PDF document parsing using LlamaParse for policy ingestion.

| Function | Description |
|----------|-------------|
| `parse_all_policy_documents` | Parse all PDFs in directory to combined Markdown |
| `parse_single_policy_document` | Parse a single PDF document |

---

## 🛠️ Tech Stack

<table>
<tr>
<td>

**AI/ML**
- Amazon Nova API (Native)
- `strands-amazon-nova` SDK
- Multi-Agent System

</td>
<td>

**Databases**
- PostgreSQL (psycopg2)
- Neo4j Aura

</td>
<td>

**Cloud & Hosting**
- AWS App Runner (Backend)
- AWS Amplify (Frontend)
- Google Cloud (Email Ingestion)
- Cloud SQL Postgres

</td>
<td>

**Frameworks**
- FastAPI
- uvicorn
- FastMCP (mcp[cli])
- SSE-Starlette

</td>
<td>

**Processing**
- LlamaParse
- pypdf
- BeautifulSoup4
- Pillow

</td>
</tr>
</table>

---

## 📁 Project Structure

```
VARA_AI_NOVA/
│
├── gmail-event-processor/           # 📧 Email Ingestion Service
│   ├── app.py                       # FastAPI Pub/Sub endpoint
│   ├── classifier.py                # Nova email classification
│   ├── gmail_processor.py           # Gmail API integration
│   ├── store_email.py               # Storage queue
│   ├── secret_manager.py            # Credentials management
│   ├── Dockerfile
│   └── requirements.txt
│
├── mcp_processor/                   # ⚙️ Main Processing Service (AWS App Runner)
│   ├── app.py                       # Tasks endpoint
│   ├── processor.py                 # MCPProcessor orchestrator
│   ├── Dockerfile
│   └── requirements.txt
│
├── policy_compiler_agents/          # 🤖 Multi-Agent System
│   ├── agent.py                     # Pipeline orchestrator
│   ├── ontology_agent.py            # Graph schema design
│   ├── extraction_agent.py          # Entity & relationship extraction
│   ├── critic_agent.py              # Quality validation
│   ├── adjuciator_agent.py          # Decision making mechanism
│   ├── graph_traversal.py           # Policy graph traversal
│   ├── source_retrieval.py          # Citation lookup
│   ├── visualize_graph.py           # Graph visualization
│   ├── ingestion.py                 # Document ingestion
│   └── tools.py                     # Shared utilities
│
├── db_verification/                 # 🗄️ MCP Server - Database
│   ├── db_verification_server.py    # MCP tools for order lookup
│   ├── db.py                        # Cloud SQL Postgres connector
│   └── llm_sql_runner.py            # Natural language SQL
│
├── doc_server/                      # 📄 MCP Server - Document Processing
│   └── mcp_doc_server.py            # Invoice PDF parsing
│
├── defect_analyzer/                 # 🔍 MCP Server - Defect Analysis
│   └── mcp_server.py                # Nova Vision defect analysis
│
├── neo4j_graph_engine/              # 🔷 Neo4j Graph Operations
│   ├── mcp_server.py                # Graph query functions
│   └── db.py                        # Neo4j async driver
│
├── policy_engine/                   # 📚 Policy Document Parser
│   └── mcp_server.py                # LlamaParse integration
│
├── knowledge_base_server/           # 🌐 Policy Compiler Web Service (AWS App Runner)
│   ├── main.py                      # FastAPI server with SSE
│   ├── compiler_service.py          # Compilation orchestration
│   └── static/                      # Web UI assets
│
├── vara-ai-frontend/                # 🖥️ Web Dashboard UI (AWS Amplify)
│   └── public/                      # HTML/CSS/JS frontend files
│
├── bedrock_client.py                # ✨ Shared Amazon Nova API Handler
├── mcp_client.py                    # 🧪 Active MCP client routing
├── requirements.txt                 # Python dependencies
├── Dockerfile                       # Container root base
└── README.md                        # Project documentation
```

---

## 🚀 Local Development Setup

### Prerequisites

- Python 3.11+
- AWS Credentials / Nova Access tokens
- Neo4j Aura instance
- Google Cloud account with enabled APIs
- LlamaParse API key

### Installation

```bash
# Clone the repository
git clone https://github.com/gagan0116/vara-ai.git

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```env
# Amazon Nova Models
AWS_REGION=us-east-1
# Set this for Native Nova API access:
AWS_BEARER_TOKEN_BEDROCK="your_api_token"
NOVA_MODEL_PRO=us.amazon.nova-pro-v1:0
NOVA_MODEL_LITE=us.amazon.nova-lite-v1:0

# Neo4j Aura
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password

# Cloud SQL (PostgreSQL)
CLOUD_INSTANCE=project:region:instance
CLOUD_DB_NAME=refunds_db
CLOUD_DB_USER=postgres
CLOUD_DB_PASS=your_db_password

# LlamaParse
LLAMA_CLOUD_API_KEY=your_llamaparse_key

# Google Cloud Storage
GCS_BUCKET_NAME=your_bucket_name
```

### Running Locally

```bash
# Run the Main MCP Testing client
python mcp_client.py

# Run the Policy Compiler web service
cd knowledge_base_server
python main.py
```

---

## 👥 Contributors

| Name | GitHub |
|------|--------|
| **Gagan Vadlamudi** | [@gagan0116](https://github.com/gagan0116) |
| **Naga Sai Satish Amara** | |

---

## 🔗 Links

- **GitHub:** [https://github.com/gagan0116/vara-ai.git](https://github.com/YourUsername/vara-ai)
- **Live Demo:** [https://staging.d1vug68j94viep.amplifyapp.com/](https://staging.d1vug68j94viep.amplifyapp.com/)