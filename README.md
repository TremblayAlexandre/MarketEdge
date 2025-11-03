# 📊 Predict S&P 500 with New Laws

**From Regulation to Investment Decision — in Under 3 Minutes**

---

## 🧩 Overview

**Predict S&P 500 with New Laws** is an AI-driven decision-support system that analyzes new or existing **laws and regulations**, evaluates their **market impact**, and generates **investment recommendations** across the S&P 500.

The system connects **legislative text**, **corporate filings**, and **market data** to show which companies or sectors are most exposed, benefiting, or at risk.

---

## ⚙️ Architecture

| Layer                    | Stack / Tools                                                            | Description                                                                                   |
| ------------------------ | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| **Frontend (UI)**        | React 19 + Vite + Tailwind + shadcn/ui                                   | Displays results, heatmaps (TradingView-style), and company impact visualizations             |
| **Backend (API)**        | AWS Lambda (Python 3.12) + API Gateway                                   | Handles requests from the UI, calls LLM and Comprehend analysis pipelines                     |
| **AI / NLP Pipeline**    | LangChain + FinBERT (ProsusAI/finbert) + AWS Bedrock + Amazon Comprehend | Summarization, sentiment, entity, and risk extraction from laws and 10-K/10-Q sections        |
| **Data Layer**           | SEC-Parser (alphanome-ai) + AWS Aurora (PostgreSQL) + S3                 | Collects and structures filings (10-K, 10-Q) for S&P 500 companies                            |
| **Decision Engine**      | AWS Q Business / SageMaker Studio                                        | Combines analysis outputs into buy/sell scores ∈ [-1, 1] with reasoning and confidence levels |
| **Monitoring & Logging** | AWS CloudWatch                                                           | Tracks Lambda execution, pipeline errors, and performance metrics                             |

---

## 🔧 What is Inside the Lambda at MarketEdge?

The Lambda architecture is built as a modular, event-driven microservices system with seven specialized handlers:

**Router (`lambda_function.py`)**: Generic event router handling both HTTP (API Gateway) and SQS messages. Routes `/api/<action>` requests to appropriate handlers and manages async job polling via `/api/status/{job_id}`.

**Document Analysis (`analyse.py`)**: Hybrid HTTP/SQS handler for law extraction and AI synthesis. HTTP requests immediately return a `job_id` and queue work to SQS; workers extract text (Textract for PDFs, regex for HTML/XML), normalize via multi-language translation, then invoke Claude Haiku (Bedrock) with structured tool-use to generate sector impact scores, macro/micro tags, and confidence metrics. Results stored in S3.

**Enrichment (`enhance.py`)**: Augments law analysis with domain-specific tagging and NLP insights. Matches tags against `compiled_domain_tags.json`, classifies sectors into impact buckets (strong_positive/negative, moderate_positive/negative) using configurable thresholds, and optionally runs Amazon Comprehend for sentiment, entities, and key phrases.

**Stock Mapping (`lookup.py`)**: Maps regulatory impacts to S&P 500 companies. Queries Aurora for company financials and sector exposures, calls Claude Haiku to synthesize sentiment × regulatory impact × sector risk into `PredictedPosition` scores ∈ [-1, 1], and stores per-ticker decisions with confidence levels.

**Decision Synthesis (`decision.py`)**: Generates executive summaries. Calls Claude Sonnet to synthesize market outlook, sector winners/losers, and investment strategy in multiple modes (summary/detailed/executive) and languages (EN/FR). Results persisted to DynamoDB with 7-day TTL.

**Conversation (`chat.py`)**: Multi-turn chat with DynamoDB session persistence. Embeds regulatory analysis context (12K+ tokens) in system prompt, maintains chat history via LLM-summarization of older turns, and dynamically loads analysis from S3/DynamoDB or request body.

**Utilities (`unified_extract_and_translate.py`)**: Text normalization, language auto-detection, and parallel translation via Amazon Translate. Handles encoding issues and chunks large documents for concurrent processing.

**Data Flow**: HTTP/SQS → Router → Handler modules → (Bedrock LLM) → S3/DynamoDB → Polling client or chat session. Asynchronous pattern ensures Lambda never times out on long analyses.

---

## 🧠 Pipeline Overview

### 1️⃣ Law Analysis

- Upload any legislative or regulatory document (HTML, PDF, TXT, XML).
- Extract key entities (sectors, companies, economic indicators) via **Amazon Comprehend**.
- Summarize and categorize affected sectors and themes (e.g., energy policy, tech regulation).

### 2️⃣ Financial Context Integration

- Parse **10-K** and **10-Q** filings using **SEC-Parser**.
- Identify **Part II Item 7 — MD&A** and other relevant sections.
- Run **FinBERT** sentiment and tone analysis (neutral/positive/negative with probabilities).
- Cross-reference mentions with law topics to build a **sector impact matrix**.

### 3️⃣ Investment Decision Model

For each ticker, combine:

- `sentiment` (FinBERT score)
- `law_impact_score` (from Comprehend entity mapping)
- `risk_diff` (difference vs. sector average)

Formula:

```math
final_score = w1 * sentiment + w2 * law_impact + w3 * risk_diff
```

Decision Rules:

| Range    | Action         | Meaning                   |
| -------- | -------------- | ------------------------- |
| > 0.5    | 🟩 Strong Buy  | Expected benefit from law |
| 0 – 0.5  | 🟢 Buy         | Mild positive impact      |
| -0.5 – 0 | 🔴 Sell        | Moderate risk             |
| < -0.5   | 🟥 Strong Sell | High regulatory risk      |

### 4️⃣ Visualization / UI

- React heatmap using `react-trading-view-heatmap` or `@antv/g6` for interactive sector maps.
- Color intensity reflects the **buy/sell signal** and **confidence level**.
- Clicking a cell opens the company summary, FinBERT tone, and relevant law sections.

---

## 🧪 Setup & Deployment

### Prerequisites

- AWS Account with Bedrock, Comprehend, Lambda, Aurora, and S3 enabled
- Node.js ≥ 20 and Python ≥ 3.12
- Docker (optional for local testing)

---

### 🖥️ Backend Setup

```bash
# Create virtual environment
python3 -m venv venv && source venv/bin/activate

# Install dependencies
pip install boto3 langchain transformers sec-parser awscli

# Deploy Lambda (example)
zip -r lambda.zip lambda_handler.py
aws lambda create-function   --function-name predict-snp500-law   --zip-file fileb://lambda.zip   --handler lambda_handler.lambda_handler   --runtime python3.12   --role arn:aws:iam::<your-account-id>:role/lambda-exec-role
```

---

### 💻 Frontend Setup

```bash
# Clone project
git clone https://github.com/<your-org>/predict-snp500-laws.git
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

---

## 🧩 Example Workflow

1. Upload a **law document** through the UI.
2. The backend extracts entities and summarizes the law.
3. The AI pipeline cross-references 10-K / 10-Q data.
4. The decision engine computes buy/sell signals.
5. The React heatmap updates with color-coded market impact.

---

## 📈 Example Output (JSON)

```json
{
  "AAPL": {
    "sector": "Information Technology",
    "sentiment": 0.72,
    "law_impact": 0.55,
    "risk_diff": 0.18,
    "final_score": 0.62,
    "decision": "BUY"
  },
  "JPM": {
    "sector": "Financials",
    "sentiment": -0.43,
    "law_impact": -0.27,
    "risk_diff": -0.11,
    "final_score": -0.52,
    "decision": "STRONG_SELL"
  }
}
```

---

## 📡 Monitoring & Logs

- **AWS CloudWatch** captures logs from Lambda and SageMaker jobs.
- Log metrics include: latency, throughput, token usage, and model accuracy.
- Alerts can be set on **execution failures** or **negative confidence deltas**.

---

## 🧠 Research Highlights

- Combines **financial NLP (FinBERT)** with **regulatory text summarization**.
- Bridges **law interpretation** and **equity risk prediction**.
- Enables faster **investment decision-making** for analysts and policymakers.

---

## 🛠️ What Have We Worked On to Get Here?

The road to MarketEdge involved _a lot_ of experimentation, iteration, and technological dead ends (the good kind). Here's the messy, beautiful journey:

**Early Exploration & Domain Tagging:**

- **AWS Comprehend** — Entity extraction, key phrase detection, sentiment analysis (industry classification baseline)
- **Domain Tag Compilation** — `compile_domain_tags.py` aggregating tags across 500+ company datasets with frequency analytics
- **Improved Comprehend Tagger** — Keyword-based classification bootstrapping (pharmaceutical, biotech, cloud, fintech, etc.)
- **Final Domain Tagger** — Enhanced industry mappings with 30+ sector categories and hand-curated company classifications
- **Regex-based Classification** — Pattern matching for sectors, regulatory keywords, and entity recognition

**Financial Data & Sector Analysis:**

- **yfinance Integration** — Yahoo Finance for real-time company metadata, market caps, and industry classification
- **SEC-EDGAR Downloader** — Direct API retrieval of 10-K and 10-Q filings (foundation for company exposure mapping)
- **Detailed Risk Analysis** — Parsing MD&A sections and extracting quantifiable risk factors from Part 1A of SEC filings
- **Create Individual Sectors** — Multi-sector company mappings (e.g., Fintech = Financials + Information Technology)
- **S&P 500 Composition CSV** — Master ticker dataset with sector and industry benchmarking

**Weight & Impact Scoring:**

- **Smart Weights Calculator** — LLM-based weighting system using Bedrock (Claude Sonnet) to assign business relevance scores to domain tags
- **Multi-factor Decision Models** — Combining sentiment × law impact × sector risk into normalized buy/sell signals ∈ [-1, 1]
- **Confidence Metrics** — Experimenting with data completeness, legal text similarity, and explainability scoring
- **Improved Analysis System** (`lambda_wip/`) — Advanced quantitative modeling with regulatory exposure quantification, scenario analysis with probabilities, peer cohort comparison

**Document Processing & Multilingual Support:**

- **AWS Textract** — PDF text extraction with OCR for scanned and digital documents (handles encoded PDFs and images)
- **HTML/XML Parsing** — Regex-based and ElementTree extraction for legislative HTML/XML documents
- **Amazon Translate** — Auto-language detection with parallel chunked translation for non-English regulations (French, Chinese, Japanese directives)
- **Text Normalization** — Handling encoding issues, control characters, multi-language formatting quirks, and spacing preservation

**Exploratory Notebooks & Rapid Iteration:**

- **`decision_lambda.ipynb`** — Early Lambda testing, Bedrock API patterns, and financial synthesis examples
- **`findberg.ipynb`** — FinBERT experimentation for sentiment analysis (foundation for later Bedrock migration)
- **Jupyter-based Prototyping** — Interactive data exploration before productionizing into Lambda modules
- **Lambda WIP (`lambda_wip/`)** — Parallel development tracks with `working_pdf/` subfolder for PDF extraction edge cases

**What We Ditched (& Why):**

- ❌ **OpenAI API** — Cost per token + latency concerns → switched to AWS Bedrock (cheaper at scale, lower latency)
- ❌ **Local Transformer Models** — Lambda cold-start overhead (10+ seconds) → Claude via Bedrock (pre-loaded infrastructure)
- ❌ **Hardcoded Company-Sector Mappings** — Fragile and static → LLM synthesis for flexibility and accuracy
- ❌ **Synchronous End-to-End Processing** — Lambda 15-min timeout walls → SQS async pattern with job polling
- ❌ **Single-Pass Analysis** — Too simplistic → Multi-stage pipeline (extract → enhance → lookup → decide → chat)
- ❌ **Comprehend-Only NLP** — Limited sentiment nuance → Moved to LLM-based reasoning with Comprehend as optional augmentation

**The Final Stack:**
Claude Haiku (regulation analysis) + Claude Sonnet (synthesis) + Amazon Comprehend (NLP baseline) + AWS Textract (PDF extraction) + Amazon Translate (i18n) + DynamoDB (session persistence) + Aurora PostgreSQL (S&P 500 master data) + S3 (async results) + CloudWatch (observability) = MarketEdge ✨

---

## 📡 External Tools & GitHub

- SEC-EDGAR — Source of official filings data used to retrieve 10-K and 10-Q reports.
- SEC-API — Official EDGAR / SEC data API (GitHub): https://github.com/sec-api/sec-api
- SEC-Parser (alphanome-ai) — Filings parser used in Data Layer (GitHub): https://github.com/alphanome-ai/sec-parser

_Note: These repositories provide the upstream tools for fetching and parsing U.S. SEC filings used by this project._

---

## �👩‍💻 Authors & Contributors

- **Sarah Ait-Ali-Yahia**
- **Samai Azimi**
- **Massil Serik**
- **Alexandre Tremblay**

---

## 📜 License

MIT License © 2025 — Predict S&P 500 with New Laws
