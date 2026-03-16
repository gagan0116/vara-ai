# VARA.ai

![Amazon Nova](https://img.shields.io/badge/Amazon%20Nova-Powered-blue?style=for-the-badge&logo=amazon)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python)
![MCP](https://img.shields.io/badge/MCP-Model%20Context%20Protocol-purple?style=for-the-badge)
![Multi-Agent](https://img.shields.io/badge/Multi--Agent-5%20Agents-orange?style=for-the-badge)
![Neo4j](https://img.shields.io/badge/Neo4j-Aura-008CC1?style=for-the-badge&logo=neo4j)

> **A multi-agent customer support platform built with Amazon Nova, Neo4j, and MCP that delivers verified refund decisions with grounded, explainable reasoning from a policy knowledge graph.**

### 🏆 VARA AI (Powered by Amazon Nova)

---

## 🏆 Amazon Nova Hackathon Submission

- **Submitter Type**: Team (Professional / Student)
- **Category**: Agentic AI / Multimodal Understanding
- **Text description**: VARA AI automates customer support refund workflows by orchestrating multiple agents powered by Amazon Nova's advanced reasoning (Nova 2 Pro) and multimodal understanding (Nova 2 Lite). The system synthesizes text, PDF invoices, and defect images, performs an agentic database verification loop using MCP tools, and references a Neo4j policy knowledge graph to output a verified, explainable decision in 90 seconds.
- **Demo Video**: [YouTube Demo](#) #AmazonNova
- **Code repo**: [GitHub Repository](https://github.com/gagan0116/mcp_customer_support)
- **Live Demo**: [Public URL](https://storage.googleapis.com/mcp_frontend/index.html) (No login required)
- **Bonus Blog Post**: [Read on builder.aws.com](#) <!-- Update with actual blog link -->

---

## 💡 Inspiration

Refunds are one of those moments where trust either gets cemented—or quietly dies.

It usually starts small: a damaged item, a missing part, the wrong SKU. The customer doesn’t wake up wanting a fight; they just want it made right. But the second they hit “Contact Support,” time starts stretching. They attach the invoice, maybe a couple of photos, explain the issue… and then they wait. And wait. Somewhere on the other side, an agent is juggling dozens of tickets, trying to interpret blurry images, open PDFs that don’t parse cleanly, cross-check order records, and translate a policy document into a decision that won’t get them in trouble later.

The customer feels ignored. The company feels attacked. And the most frustrating part is that neither side is being unreasonable—the process is.

This isn’t a corner case either. Returns are massive and messy at scale: U.S. retailers estimate that **15.8% of annual sales will be returned in 2025 — roughly $849.9B** ([NRF](https://nrf.com/media-center/press-releases/consumers-expected-to-return-nearly-850-billion-in-merchandise-in-2025)). And the pressure isn’t only operational—**return fraud is ~9% of all returns**, costing about **$76.5B annually** ([Reuters](https://www.reuters.com/business/retail-consumer/ups-company-deploys-ai-spot-fakes-amid-surge-holiday-returns-2025-12-18/?utm_source=chatgpt.com)).

We built **VARA AI** because the refund experience shouldn’t be a slow, opaque negotiation. It should feel like: *“We received your request, we understand what happened, here’s the decision, here’s why, and here’s what happens next.”* Fast enough that customers don’t have to chase—and accountable enough that companies can trust the outcome.

**Speed without guesswork, automation without losing trust.**

---

## 🎯 What it does

VARA AI automates the refund customer support workflow from intake to decision. It processes a refund request in approximately **90 seconds** and produces an outcome that is **grounded in evidence** and **explainable**.

For each refund request, VARA AI:
- **Ingests the case** from the customer email and attachments (invoice PDFs and defect images).
- **Extracts structured information** from documents (PDF parsing & Vision analysis).
- **Verifies key fields** against backend records via a database verification service.
- **Retrieves relevant policy context** using GraphRAG over a structured policy knowledge graph.
- **Generates a decision**: **Approve / Deny / Manual Review**, including the refund action or next step.
- **Produces an explanation** that references the evidence used and the policy basis for the outcome.

---

## 🔷 Amazon Nova Features Implemented

VARA AI leverages the cutting-edge capabilities of Amazon Nova to solve complex orchestration and reasoning challenges:

- **Deep Reasoning**  
  Enabled via `nova-2-pro-v1`. Used in the **Ontology**, **Critic**, and **Adjudicator** agents to support multi-step reasoning before generating schemas or refund decisions.
- **Thought Traces**  
  We retain Amazon Nova's output thought trace metadata to preserve reasoning context across turns and to support **auditability** of how decisions were reached.
- **High Media Resolution**  
  Configured `media_resolution="high"` in the Defect Analyzer’s vision requests to capture fine-grained defects (e.g., small scratches, hairline cracks) from customer images.
- **Structured Output & Type-Safety**  
  We use `response_schema` and `response_mime_type="application/json"` across agents to enforce strict, machine-validated outputs aligned with our **Neo4j** and **PostgreSQL** schemas.
- **Low-Latency Tool Loops**  
  We run the Database Verification Agent on `nova-2-lite-v1` to power fast, multi-turn verification loops where the model selects and executes MCP tools.
- **Multimodal Perception & Synthesis**  
  We merge Amazon Nova's visual findings from defect images with invoice fields and email context into a single evidence bundle for grounded adjudication.

---

## 🏗️ How we built it

We built VARA AI as two connected pipelines: an offline policy compiler that turns messy T&Cs into a traceable Neo4j knowledge graph, and an online adjudication workflow that processes each refund email end-to-end.

### 1. Policy Knowledge Graph Pipeline (GraphRAG)
![Architectural Diagram of the Multi-Agent Knowledge Graph Pipeline](https://storage.googleapis.com/markdown_imgs/Multi%20Agent%20Policy%20Knowledge%20Graph%20Pipeline.png)

A multi-agent “policy compiler” converts unstructured T&C PDFs into a validated Neo4j policy graph. Highlights:
- **Ontology Agent**: Designs the graph schema (node/relationship types).
- **Extraction Agent**: Performs 3-phase triplet extraction and deterministic linking.
- **Critic Agent**: Validates schema coverage and Cypher quality via a self-correction loop.

### 2. End-to-end Refund Workflow (Email → Decision)
![End-to-end workflow: email intake to adjudication and response](https://storage.googleapis.com/markdown_imgs/end-to-end%20Refund%20Workflow.png)

Once the policy graph exists, the runtime workflow processes each incoming customer email:
1. **Intake**: Gmail Watch triggers Cloud Run to ingest email and attachments into GCS.
2. **Analysis**: MCP servers process invoice PDFs and analyze defect images (Amazon Nova Vision).
3. **Verification**: Agentic DB loop (Postgres) resolves order records deterministically.
4. **Adjudication**: Amazon Nova adjudicates the case by traversing the Neo4j policy graph (Grounded Reasoning).
5. **Audit**: Every stage persists artifacts for full reproducibility.

---

## 🛠️ Google Tech Stack

![Google Technology Stack used in VARA AI](https://storage.googleapis.com/markdown_imgs/Googl's%20Tech%20Stack%20.png)

**Core Infrastructure:**
- **AI**: Amazon Nova (Pro & Lite)
- **Framework**: Model Context Protocol (MCP), FastMCP, FastAPI
- **Storage/DB**: Google Cloud Storage, Cloud SQL (PostgreSQL), Neo4j AuraDB, Firestore
- **Compute**: Cloud Run, Cloud Tasks, Cloud Build
- **Events**: Cloud Pub/Sub, Gmail API

---

## 📈 Accomplishments & Learnings

### What we're proud of
- **End-to-end automation**: A single return email triggers a machine-verified outcome in 90s.
- **Policy Ingestion**: Transforming messy PDFs into a structured, citation-backed knowledge graph using multi-agent orchestration.
- **UX Excellence**: Providing a guided dashboard that makes complex AI processes transparent.

### Challenges overcome
- **Rate Limits (429s)**: Implemented robust backoff and retry logic for multi-stage model calls.
- **Explainability**: Using GraphRAG to ensure every decision is defensible and grounded in actual policy text rather than "hallucinated" general rules.
- **Unstructured Data**: Developing custom parsing strategies for inconsistent retail T&C documents.

---

## 🚀 Future Roadmap

- **Voice-based Support**: Adding a voice interface for refund intake while keeping the same robust reasoning backend.
- **Two-way Automation**: Enabling the system to follow up with customers for missing info (e.g., "Please upload a clearer image of the scratch").
- **Risk & Fraud Scoring**: Integrated analysis of return history to flag high-risk cases for human agents.

---

## 📁 Project Structure

```
mcp_customer_support/
├── gmail-event-processor/   # 📧 Email Ingestion Service
├── mcp_processor/           # ⚙️ Main Processing Orchestrator
├── policy_compiler_agents/  # 🤖 Multi-Agent Graph Builder
├── db_verification/         # 🗄️ MCP Server - Database
├── doc_server/              # 📄 MCP Server - Document Processing
├── defect_analyzer/         # 🔍 MCP Server - Vision Analysis
├── neo4j_graph_engine/      # 🔷 Neo4j Operations
├── vara-ai-frontend/        # 🖥️ Web Dashboard UI
└── scripts/                 # 🛠️ Setup & Utility Scripts
```

---

## 👥 Contributors

- **Gagan Vadlamudi** ([@gagan0116](https://github.com/gagan0116))
- **Naga Sai Satish Amara**

---
*Migrated to Amazon Nova, 2026.*
