# 📊 Predict S&P 500 with New Laws  
**From Regulation to Investment Decision — in Under 3 Minutes**

---

## 🧩 Overview  
**Predict S&P 500 with New Laws** is an AI-driven decision-support system that analyzes new or existing **laws and regulations**, evaluates their **market impact**, and generates **investment recommendations** across the S&P 500.  

The system connects **legislative text**, **corporate filings**, and **market data** to show which companies or sectors are most exposed, benefiting, or at risk.

---

## ⚙️ Architecture  

| Layer | Stack / Tools | Description |
|-------|---------------|-------------|
| **Frontend (UI)** | React 19 + Vite + Tailwind + shadcn/ui | Displays results, heatmaps (TradingView-style), and company impact visualizations |
| **Backend (API)** | AWS Lambda (Python 3.12) + API Gateway | Handles requests from the UI, calls LLM and Comprehend analysis pipelines |
| **AI / NLP Pipeline** | LangChain + FinBERT (ProsusAI/finbert) + AWS Bedrock + Amazon Comprehend | Summarization, sentiment, entity, and risk extraction from laws and 10-K/10-Q sections |
| **Data Layer** | SEC-Parser (alphanome-ai) + AWS Aurora (PostgreSQL) + S3 | Collects and structures filings (10-K, 10-Q) for S&P 500 companies |
| **Decision Engine** | AWS Q Business / SageMaker Studio | Combines analysis outputs into buy/sell scores ∈ [-1, 1] with reasoning and confidence levels |
| **Monitoring & Logging** | AWS CloudWatch | Tracks Lambda execution, pipeline errors, and performance metrics |

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

| Range | Action | Meaning |
|--------|---------|----------|
| > 0.5 | 🟩 Strong Buy | Expected benefit from law |
| 0 – 0.5 | 🟢 Buy | Mild positive impact |
| -0.5 – 0 | 🔴 Sell | Moderate risk |
| < -0.5 | 🟥 Strong Sell | High regulatory risk |

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

## 👩‍💻 Authors & Contributors  
- **Sarah A.**
- **Samai Azimi** 

---

## 📜 License  
MIT License © 2025 — Predict S&P 500 with New Laws
