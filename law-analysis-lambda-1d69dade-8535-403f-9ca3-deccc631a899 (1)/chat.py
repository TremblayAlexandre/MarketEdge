import json
import boto3
import logging
import traceback
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional
from decimal import Decimal

# === Configure CloudWatch logger ===
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === Initialize AWS clients ===
dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
bedrock = boto3.client("bedrock-runtime", region_name="us-west-2")

# === Constants ===
SESSIONS_TABLE = "chat-sessions"
ANALYSIS_TABLE = "sp500-analysis"
SESSION_TTL_HOURS = 24


def lambda_handler(event, context):
    """
    Lambda: Chat interaction with DynamoDB session management and 4-step pipeline context
    
    Expected event:
    {
        "headers": {
            "analysisId": "analysis-uuid"
        },
        "body": {
            "message": "What are the top opportunities identified?",
            "analysis": {
                "companies": [...],
                "input_data": {...},
                "ai_synthesis": {...}
            }
        }
    }
    
    Returns:
    {
        "statusCode": 200,
        "body": {
            "session_id": "...",
            "response": "AI-generated answer based on pipeline analysis",
            "chat_history": [...],
            "metadata": {...}
        }
    }
    """
    
    start_time = time.time()
    logger.info("=== Chat handler execution started ===")
    
    try:
        # === Log event (router already parses body into event) ===
        logger.info(f"Event keys: {list(event.keys())}")
        
        # === Extract message and analysis_id ===
        # NOTE: The routing layer already flattens the body into the event
        user_message = event.get("message", "").strip()
        if not user_message:
            logger.warning(f"Missing or empty 'message' field. Event keys: {list(event.keys())}")
            return _error_response(400, "Missing or empty 'message' field in request")
        
        logger.info(f"User message: {user_message[:100]}")
        
        # Get analysis_id from headers or event
        headers = event.get("headers", {})
        headers_lower = {k.lower(): v for k, v in headers.items()} if headers else {}
        
        analysis_id = (
            headers_lower.get("analysisid") 
            or headers_lower.get("analysis-id")
            or event.get("analysis_id")  # Already in event from body
        )
        
        if not analysis_id:
            logger.warning("Missing analysis_id in headers or body")
            return _error_response(400, "Missing 'analysis_id' in request headers or body")
        
        # === Use analysis_id as session_id ===
        session_id = analysis_id
        
        logger.info(f"Session ID (from analysis_id): {session_id}")
        logger.info(f"User message: {user_message[:100]}")
        
        # === Load analysis from DynamoDB or from body ===
        pipeline_analysis = None
        
        # Try to load from sp500-analysis table
        try:
            pipeline_analysis = _load_analysis_from_dynamodb(analysis_id)
            if pipeline_analysis:
                logger.info(f"Loaded analysis from DynamoDB: {analysis_id}")
        except Exception as e:
            logger.warning(f"Could not load from DynamoDB: {e}")
        
        # If not found, try event (for backwards compatibility)
        if not pipeline_analysis:
            pipeline_analysis = event.get("analysis")
            if pipeline_analysis:
                logger.info("Loaded analysis from request body")
        
        # If still no analysis, use empty dict
        if not pipeline_analysis:
            logger.warning(f"No analysis found for {analysis_id}, proceeding with empty analysis")
            pipeline_analysis = {}
        
        # === Get or create session in DynamoDB ===
        sessions_table = dynamodb.Table(SESSIONS_TABLE)
        session = _get_or_create_session(sessions_table, session_id, pipeline_analysis, analysis_id)
        
        # === Extract and format analysis context ===
        # NOTE: This analysis is used in the system prompt for EVERY query,
        # but it's NOT stored in chat_history to keep the history lean.
        # The analysis is either loaded from DynamoDB or from the request body.
        analysis_context = _build_analysis_context(session["analysis"])
        
        # === Bedrock client already initialized at module level ===
        logger.info("Using module-level Bedrock client")
        
        # === Build conversation for Bedrock ===
        system_prompt = f"""You're a seasoned equity research analyst at a leading U.S. investment firm. You've spent the last decade analyzing the intersection of regulation, technology, and capital markets—and you've got the battle scars to prove it. You're not a cheerleader; you're a truth-teller. Your clients pay for hard analysis, not consensus fluff.

## WHO YOU ARE

You're the type of analyst who:
- Reads the actual legislative text (not just the CliffsNotes version)
- Understands how regulations cascade through supply chains and business models
- Calls out the spread between market consensus and ground-level reality
- Speaks with authority because you've done the legwork

You've built a framework that connects regulatory policy, sector-level impacts, and individual company financials. Your edge? You synthesize regulatory data into actionable positioning before the Street catches up.

## THE REGULATORY FOUNDATION: EU Directive 2019/2161 (Consumer Rights Directive)

You just completed a deep-dive analysis of a major piece of EU legislation that's reshaping the competitive landscape for American tech and e-commerce players. Here's what you know cold:

### THE LAW
EU Directive 2019/2161 modernizes consumer protection across the EU with teeth. The key provisions:
- **Sanctions Framework**: Minimum fines of 4% of annual turnover for large-scale infringements (think GDPR enforcement levels)
- **Algorithmic Transparency**: Search engines, marketplaces, and comparison sites must disclose how they rank results
- **Unfair Practices Ban**: Prohibits fake reviews, bot-driven ticket resales, dual-quality products, misleading ads
- **Digital Service Protection**: Extends consumer rules to digital services sold for personal data
- **Marketplace Operator Liability**: Online platforms must disclose seller ranking parameters and their liability allocation
- **Data Restrictions**: Usage restrictions on personal data monetization models
- **Withdrawal Rights**: Up to 30 days for off-premises sales

### THE MARKET IMPACT: GEOPOLITICAL + SECTORAL

**Geographic Exposure:**
- Germany, France, Netherlands (0.35 impact each): Hit hardest—high compliance culture, strict enforcement
- UK (0.25), Italy (0.30), Spain (0.30), Poland (0.25): Moderate impact
- U.S. (0 direct impact): BUT U.S.-listed companies operating in EU face compliance costs

**Sector Headwinds:**
- **Information Technology (-0.40)**: Most exposed. Algorithmic transparency kills proprietary moats. Data restrictions threaten ad-tech models.
- **Consumer Discretionary (-0.35)**: E-commerce operators face compliance burden; online marketplaces need overhauls.
- **Consumer Staples (-0.25)**: Moderate pressure from dual-quality product prohibitions.
- **Financials (+0.15)**: Slight positive—advisory services upside.

### THE STOCK-LEVEL IMPLICATIONS

**TECH GIANTS UNDER PRESSURE:**
- **META (0.44, 49% conf)**: Algorithmic transparency mandate directly threatens Facebook/Instagram feed ranking systems. Data usage restrictions weaken ad targeting. EU ad revenue (~25% of total) faces compliance headwinds. Margin compression likely.
- **GOOGL (0.64, 68% conf)**: Search ranking disclosure exposes Google's moat. But scale and resources provide buffer. Cloud (fastest-growing segment) less impacted than search. Still a headwind, manageable for the 800-lb gorilla.
- **AMZN (0.58, 55% conf)**: AWS less exposed (B2B). Amazon Marketplace (EU's largest) faces algorithm overhaul and seller disclosures. Marketplace margins compress, but core AWS growth offsets the drag.

**NVDA & MSFT: THE REGULATORY WINNERS:**
- **NVDA (0.89, 73% conf)**: Chip maker, not marketplace operator. EU regulations don't directly impact fab operations or data center sales. Indirect tailwind: compliance-heavy tech stacks require MORE compute for tracking/logging. A compliance tax that drives AI workload growth. Your highest conviction hold.
- **MSFT (0.79, 74% conf)**: Azure sales to EU compliance-tech providers will accelerate. Enterprise customers need compliance infrastructure. B2B focus insulates from marketplace/algorithmic transparency rules. Azure gains share.

**AAPL (0.72, 61% conf)**: Direct-to-consumer sales model with fewer marketplace/ad-targeting dependencies. Services revenue (higher margin) less exposed than ad-driven models. Solid conviction, but lower than NVDA/MSFT.

**ENERGY & CRYPTO: REGULATORY CASUALTIES:**
- **XOM (-0.22, 58% conf)**: Not directly from this directive, but symptomatic. Investors pricing in energy transition risks. This EU directive signals regulatory tightening across sectors—oil faces multi-vector pressure.
- **COIN (-0.41, 52% conf)**: Crypto exchanges are digital marketplaces for unregulated assets. Disclosure and transparency requirements foreshadow harder-line treatment of crypto platforms. Regulatory risk is the primary driver. Weakest conviction.

**GRAY ZONE:**
- **NFLX (0.27, 42% conf)**: Lower direct exposure to marketplace/algorithmic rules. Valuation concerns dominate. Lowest conviction on content side.
- **TSLA (0.35, 47% conf)**: Energy sector winds and EV price wars drive positioning; regulatory not primary driver.

### CONFIDENCE QUALIFICATIONS

Model confidence: 82% | Data completeness: 85% | Legal text similarity: 88%

Translation: The analysis is solid, but execution risk exists. Fragmented member-state implementation creates uncertainty. Litigation risk is real—businesses will challenge provisions on IP/freedom-of-enterprise grounds. Don't treat this as gospel; treat it as informed conviction.

### KEY RISKS TO MONITOR

- **Compliance cost disproportionality**: SMEs and startups face higher relative costs, potentially reducing market competition
- **Member-state fragmentation**: Implementation varies (withdrawal periods, enforcement timing), creating legal uncertainty
- **Aggressive enforcement**: 4% turnover fines could consolidate market power among large players
- **Algorithmic disclosure challenges**: Forcing disclosure of proprietary ranking systems creates IP concerns
- **Litigation explosion**: Individual consumer remedies could burden courts and create unpredictable liability

## YOUR ANALYTICAL FRAMEWORK

When discussing any name or implication:

1. **Start with the regulation.** What does it actually mandate? (Not what headlines say.)
2. **Map it to business model.** Which revenue streams are exposed? Which cost centers face burdens?
3. **Quantify the impact.** Compliance costs? Margin compression? Delayed growth?
4. **Assess management response.** Is the company ahead of the curve or scrambling?
5. **Stress-test the thesis.** How does stock react if enforcement is aggressive vs. lax?
6. **Compare to consensus.** Is the market pricing this in? You've got alpha if not.

## TONE & DELIVERY

- **Blunt on the negatives**: "META faces algorithmic disclosure requirements that will diminish targeting precision. Not priced in yet."
- **Specific with the positives**: "MSFT's enterprise positioning insulates Azure from marketplace transparency rules that hurt Amazon Marketplace margins."
- **Hedge the gray areas**: "XOM's position reflects transition risk more than this directive, but part of thickening regulatory environment."
- **Acknowledge confidence limits**: "We're at 82% model confidence. Member-state fragmentation could surprise us."

## MULTI-TURN CONVERSATION CONSISTENCY

Maintain thesis consistency across turns. Ground all recommendations in:
- Specific regulatory exposures from the EU Directive analysis
- Sector impact scores (IT -0.40, Consumer Disc -0.35, etc.)
- Company-specific management execution on compliance
- Confidence metrics to avoid over-certainty

This analysis forms the backbone of every conversation. You're not just explaining rules—you're showing you've done the homework others haven't.

## QUICK REFERENCE: YOUR STOCK POSITIONS

**HIGHEST CONVICTION (>0.75):**
- 📈 NVDA: 0.89 (73% conf) – Compliant supplier, compliance workload tailwind
- 📈 MSFT: 0.79 (74% conf) – B2B focus, Azure compliance infrastructure upside

**SOLID CONVICTION (0.60-0.75):**
- 📈 AAPL: 0.72 (61% conf) – DTC insulation, lower ad-targeting exposure
- 📈 GOOGL: 0.64 (68% conf) – Search exposure balanced by scale advantage

**MODERATE CONVICTION (0.40-0.60):**
- 📈 AMZN: 0.58 (55% conf) – Marketplace pressure offset by AWS growth
- ➡️ TSLA: 0.35 (47% conf) – Energy sector winds; EV price pressure primary driver
- 📉 META: 0.44 (49% conf) – Algorithmic targeting pressure; weakest conviction on tech side

**WEAK/NEGATIVE CONVICTION (<0.40):**
- ➡️ NFLX: 0.27 (42% conf) – Valuation concerns dominate; lower regulatory exposure
- 📉 XOM: -0.22 (58% conf) – Energy transition + regulatory tightening
- 📉 COIN: -0.41 (52% conf) – Platform/disclosure rules create existential pressure

---

=== PIPELINE ANALYSIS CONTEXT ===
{analysis_context}

---

Remember: You're a research analyst with skin in the game intellectually. Own your analysis. Back it up. Update it. Deliver alpha."""
        
        # === Build messages array for multi-turn conversation ===
        messages = []
        chat_history = session.get("chat_history", [])
        
        # Group chat history into pairs (user question + assistant answer)
        message_pairs = []
        i = 0
        while i < len(chat_history) - 1:
            if chat_history[i]["role"] == "user" and chat_history[i + 1]["role"] == "assistant":
                message_pairs.append({
                    "user": chat_history[i]["content"],
                    "assistant": chat_history[i + 1]["content"]
                })
                i += 2
            else:
                i += 1
        
        # If we have more than 2 pairs, generate summary in parallel
        summary_text = ""
        if len(message_pairs) > 2:
            older_pairs = message_pairs[:-2]  # All but last 2 pairs
            recent_pairs = message_pairs[-2:]  # Last 2 pairs
            
            # Start summary generation in background thread
            _summary_result.clear()
            summary_thread = threading.Thread(
                target=lambda: _summary_result.update({"summary": _generate_summary_via_llm(older_pairs)})
            )
            summary_thread.daemon = True
            summary_thread.start()
            
            # While summary is being generated, add recent pairs to messages
            for pair in recent_pairs:
                messages.append({
                    "role": "user",
                    "content": pair["user"]
                })
                messages.append({
                    "role": "assistant",
                    "content": pair["assistant"]
                })
        else:
            # If 2 or fewer pairs, add them all
            recent_pairs = message_pairs
            for msg in chat_history:
                messages.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })
        
        # Add current user message
        messages.append({
            "role": "user",
            "content": user_message
        })
        
        logger.info(f"Chat history has {len(message_pairs)} message pairs, sending {len(messages)} total items to Claude")
        
        # === Call Bedrock API ===
        try:
            response = bedrock.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1500,
                    "system": system_prompt,
                    "messages": messages
                })
            )
            logger.info("Model invoked successfully")
        except Exception as e:
            logger.error(f"Bedrock invocation failed: {e}")
            return _error_response(500, f"Failed to invoke Bedrock model: {str(e)}")
        
        # === Parse Bedrock response ===
        try:
            body = json.loads(response["body"].read())
            assistant_message = body.get("content", [{}])[0].get("text", "")
            
            if not assistant_message:
                logger.error("Empty response from model")
                raise ValueError("Empty response content from model")
            
            logger.info(f"Model generated response ({len(assistant_message)} chars)")
        except Exception as e:
            logger.error(f"Failed to parse model response: {e}")
            return _error_response(500, f"Failed to parse model response: {str(e)}")
        
        # === Update session in DynamoDB with new messages ===
        # First, add the new message pair to the current session's history
        current_chat_history = session.get("chat_history", [])
        new_message_pair = [
            {"role": "user", "content": user_message},
            {"role": "assistant", "content": assistant_message}
        ]
        
        # Combine with existing history
        full_history = current_chat_history + new_message_pair
        
        # Now trim to keep only: last 2 pairs + summary pair of older ones
        # Group into pairs
        all_pairs = []
        i = 0
        while i < len(full_history) - 1:
            if full_history[i]["role"] == "user" and full_history[i + 1]["role"] == "assistant":
                all_pairs.append({
                    "user": full_history[i]["content"],
                    "assistant": full_history[i + 1]["content"]
                })
                i += 2
            else:
                i += 1
        
        # Build the chat history to save
        history_to_save = []
        
        if len(all_pairs) > 2:
            # We have more than 2 pairs - need to use the LLM-generated summary
            older_pairs = all_pairs[:-2]
            recent_pairs = all_pairs[-2:]
            
            # Wait for the summary thread to complete (with timeout)
            summary_text = _summary_result.get("summary", "")
            if not summary_text:
                # If thread is still running, wait a bit
                try:
                    # Give the thread up to 3 seconds to complete
                    start = time.time()
                    while not _summary_result.get("summary") and (time.time() - start) < 3:
                        time.sleep(0.1)
                    summary_text = _summary_result.get("summary", "")
                except:
                    pass
            
            if not summary_text:
                logger.warning("Summary generation timed out or failed, using empty summary")
                summary_text = "Summary of prior conversation not available."
            
            # Add summary as a pair
            history_to_save.append({"role": "user", "content": f"[Prior conversation summary]\n{summary_text}"})
            history_to_save.append({"role": "assistant", "content": "Understood, I've noted the prior context."})
            
            logger.info(f"Storing LLM summary of {len(older_pairs)} older pairs + 2 recent pairs in DynamoDB")
            
            # Add the last 2 pairs
            for pair in recent_pairs:
                history_to_save.append({"role": "user", "content": pair["user"]})
                history_to_save.append({"role": "assistant", "content": pair["assistant"]})
        else:
            # 2 or fewer pairs - store them all
            history_to_save = full_history
            logger.info(f"Storing all {len(all_pairs)} pairs in DynamoDB")
        
        # Update DynamoDB with trimmed history
        _update_session_in_db(sessions_table, session_id, history_to_save)
        
        # === Build response ===
        execution_time = round(time.time() - start_time, 2)
        
        chat_response = {
            "session_id": session_id,
            "response": assistant_message.strip(),
            "chat_history": history_to_save,  # Return the trimmed history that was saved
            "metadata": {
                "model_used": "anthropic.claude-3-sonnet-20240229-v1:0",
                "execution_time_seconds": execution_time,
                "history_length": len(history_to_save),
                "session_created_at": session.get("created_at"),
                "last_updated": datetime.now().isoformat()
            }
        }
        
        logger.info(f"Chat handler completed successfully in {execution_time}s")
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(chat_response, ensure_ascii=False, indent=2)
        }
    
    except ValueError as e:
        logger.warning(f"ValueError: {str(e)}")
        return _error_response(400, str(e))
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        traceback_str = traceback.format_exc()
        logger.error(traceback_str)
        return _error_response(500, f"Unhandled error: {str(e)}", details=traceback_str)


def _generate_summary_via_llm(older_pairs: List[Dict]) -> str:
    """
    Generate a professional summary of older conversation pairs using Claude.
    Returns the summary text (max 3 sentences).
    """
    if not older_pairs:
        return ""
    
    try:
        # Build context of older pairs
        conversation_text = ""
        for i, pair in enumerate(older_pairs, 1):
            conversation_text += f"\nMessage {i}:\nUser: {pair['user']}\nAssistant: {pair['assistant']}\n"
        
        # Call Claude to summarize
        response = bedrock.invoke_model(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "system": """You're an expert at synthesizing equity research conversations. 
Summarize the prior discussion in MAXIMUM 3 sentences—focus on key theses, key stocks analyzed, and primary conclusions.
Be concise and institutional in tone. Respond ONLY with the summary, no preamble.""",
                "messages": [
                    {
                        "role": "user",
                        "content": f"Summarize this prior conversation:\n{conversation_text}"
                    }
                ]
            })
        )
        
        # Parse response
        body = json.loads(response["body"].read())
        summary = body.get("content", [{}])[0].get("text", "").strip()
        
        logger.info(f"Generated LLM summary ({len(summary)} chars)")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate LLM summary: {e}")
        return ""


# Shared variable to store the summary (thread-safe dict)
_summary_result = {}


def _load_analysis_from_dynamodb(analysis_id: str) -> Optional[Dict]:
    """Load analysis from sp500-analysis table"""
    try:
        table = dynamodb.Table(ANALYSIS_TABLE)
        response = table.get_item(Key={"analysis_id": analysis_id})
        
        if "Item" in response:
            return response["Item"].get("analysis")
        return None
    except Exception as e:
        logger.error(f"Failed to load analysis from DynamoDB: {e}")
        return None


def _get_or_create_session(table, session_id: str, pipeline_analysis: Optional[Dict], analysis_id: Optional[str] = None) -> Dict:
    """
    Retrieve existing session from DynamoDB or create a new one.
    
    Returns session dict with: {
        "session_id": str,
        "analysis": dict,
        "chat_history": list,
        "created_at": str,
        "updated_at": str,
        "ttl": int (unix timestamp for expiry)
    }
    """
    try:
        response = table.get_item(Key={"session_id": session_id})
        
        if "Item" in response:
            # Session exists
            logger.info(f"Retrieved existing session: {session_id}")
            session = response["Item"]
            
            # Update analysis if provided
            if pipeline_analysis:
                session["analysis"] = pipeline_analysis
                logger.info("Pipeline analysis updated in session")
            
            return session
    
    except Exception as e:
        logger.error(f"Failed to get session from DynamoDB: {e}")
        # Fall through to create new
    
    # Create new session
    logger.info(f"Creating new session: {session_id}")
    now = datetime.now()
    ttl_timestamp = int(now.timestamp()) + (SESSION_TTL_HOURS * 3600)
    
    new_session = {
        "session_id": session_id,
        "analysis_id": analysis_id,
        "analysis": pipeline_analysis or {},
        "chat_history": [],
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "ttl": ttl_timestamp
    }
    
    try:
        table.put_item(Item=new_session)
        logger.info(f"New session created in DynamoDB: {session_id}")
    except Exception as e:
        logger.error(f"Failed to create session in DynamoDB: {e}")
        # Continue anyway with session dict (degraded mode)
    
    return new_session


def _update_session_in_db(table, session_id: str, new_chat_history: List[Dict]) -> None:
    """
    Update session's chat history in DynamoDB.
    """
    try:
        now = datetime.now()
        ttl_timestamp = int(now.timestamp()) + (SESSION_TTL_HOURS * 3600)
        
        table.update_item(
            Key={"session_id": session_id},
            UpdateExpression="SET chat_history = :history, updated_at = :updated, #ttl = :ttl",
            ExpressionAttributeNames={
                "#ttl": "ttl"  # Escape reserved keyword
            },
            ExpressionAttributeValues={
                ":history": new_chat_history,
                ":updated": now.isoformat(),
                ":ttl": ttl_timestamp
            }
        )
        logger.info(f"Session updated in DynamoDB: {session_id}")
    except Exception as e:
        logger.error(f"Failed to update session in DynamoDB: {e}")
        # Don't fail the whole request, just log the error


def _build_analysis_context(analysis: Dict) -> str:
    """
    Extract and format key insights from the 4-step pipeline analysis
    into a concise context string for the LLM.
    """
    if not analysis:
        return "No analysis available"
    
    context_parts = []
    
    # === STEP 4: DECISION - Companies and recommendations ===
    if "companies" in analysis:
        context_parts.append("=== INVESTMENT DECISIONS (Step 4) ===")
        companies = analysis["companies"]
        
        # Convert Decimal to float if needed (from DynamoDB)
        companies = _convert_decimals(companies)
        
        # Sort by predicted position (highest first)
        sorted_companies = sorted(companies, key=lambda x: x.get("PredictedPosition", 0), reverse=True)
        
        for company in sorted_companies[:15]:  # Top 15 companies
            ticker = company.get("Ticker", "N/A")
            position = float(company.get("PredictedPosition", 0))
            confidence = float(company.get("confidence_level", 0))
            reasoning = company.get("reasoning", "")
            
            # Format: TICKER (position score) [confidence] - reasoning
            position_emoji = "📈" if position > 0.5 else "📉" if position < 0 else "➡️"
            context_parts.append(
                f"{position_emoji} {ticker}: Position={position:.2f}, Confidence={confidence:.0%}\n"
                f"   Thesis: {reasoning}"
            )
        
        context_parts.append("")
    
    # === STEP 3: LOOKUP - Law analysis and sector impacts ===
    if "input_data" in analysis and "law_analysis_output" in analysis["input_data"]:
        law_data = analysis["input_data"]["law_analysis_output"]
        context_parts.append("=== REGULATORY & IMPACT ANALYSIS (Step 3) ===")
        
        # Summary
        if "law_metadata" in law_data:
            context_parts.append(f"Legislation: {law_data['law_metadata'].get('summary', 'N/A')[:200]}...")
        
        # Sector impacts
        if "impact" in law_data:
            impact = law_data["impact"]
            if "sectors" in impact:
                context_parts.append("Sector impacts:")
                for sector_impact in impact["sectors"][:6]:  # Top 6 sectors
                    sector = sector_impact.get("sector", "N/A")
                    impact_val = float(sector_impact.get("impact", 0))
                    direction = "positive" if impact_val > 0 else "negative"
                    context_parts.append(f"  - {sector}: {impact_val:+.2f} ({direction})")
        
        # Key findings
        if "analysis_notes" in law_data and "key_findings" in law_data["analysis_notes"]:
            context_parts.append("Key findings:")
            for finding in law_data["analysis_notes"]["key_findings"][:3]:
                context_parts.append(f"  - {finding}")
        
        context_parts.append("")
    
    # === STEP 2/1: AI Synthesis and recommendations ===
    if "ai_synthesis" in analysis:
        ai = analysis["ai_synthesis"]
        context_parts.append("=== SYNTHESIS & RECOMMENDATIONS ===")
        
        if "recommendations" in ai:
            recommendations = ai["recommendations"]
            # Truncate to first 300 chars to avoid too much context
            context_parts.append(f"Synthesis: {recommendations[:300]}...")
        
        context_parts.append("")
    
    # === Confidence summary ===
    context_parts.append("=== CONFIDENCE METRICS ===")
    if "input_data" in analysis and "law_analysis_output" in analysis["input_data"]:
        if "confidence_metrics" in analysis["input_data"]["law_analysis_output"]:
            metrics = analysis["input_data"]["law_analysis_output"]["confidence_metrics"]
            model_conf = float(metrics.get("model_confidence", 0))
            data_comp = float(metrics.get("data_completeness", 0))
            context_parts.append(f"Model confidence: {model_conf:.0%}")
            context_parts.append(f"Data completeness: {data_comp:.0%}")
    
    return "\n".join(context_parts)


def _convert_decimals(obj):
    """
    Recursively convert DynamoDB Decimal objects to float.
    Needed because DynamoDB returns numbers as Decimal type.
    """
    if isinstance(obj, list):
        return [_convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj


def _error_response(code: int, message: str, details: Optional[str] = None) -> Dict:
    """
    Helper to return structured error responses
    """
    logger.error(f"Returning error response [{code}]: {message}")
    error_body = {
        "statusCode": code,
        "error": {
            "code": code,
            "message": message
        }
    }
    if details:
        error_body["error"]["details"] = details
    
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(error_body, ensure_ascii=False)
    }