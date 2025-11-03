import json
import boto3
import logging
import traceback
import time
from datetime import datetime
from decimal import Decimal

# === Configure CloudWatch logger ===
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === Initialize AWS clients ===
dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
bedrock = boto3.client("bedrock-runtime", region_name="us-west-2")

# === Constants ===
ANALYSIS_TABLE = "sp500-analysis"  # DynamoDB table for storing pipeline results


def lambda_handler(event, context):
    """
    Step 4: Decision synthesis - saves analysis to DynamoDB
    
    Expected event:
    {
        "analysis_id": "uuid-or-timestamp",
        "sp500_analysis": {...},
        "prompt_mode": "summary|detailed|executive",
        "language": "fr|en"
    }
    """
    
    start_time = time.time()
    logger.info("=== Decision Lambda execution started ===")
    
    try:
        # === Validate input ===
        if not isinstance(event, dict):
            raise ValueError("Event must be a JSON object")
        
        analysis_data = event.get("sp500_analysis")
        if not analysis_data:
            logger.warning("Missing field 'sp500_analysis'")
            return _error_response(400, "Missing field 'sp500_analysis'")
        
        analysis_id = event.get("analysis_id", f"analysis-{int(time.time())}")
        prompt_mode = event.get("prompt_mode", "summary")
        language = event.get("language", "en")
        
        logger.info(f"Analysis ID: {analysis_id}, Mode: {prompt_mode}")
        
        # === Compose prompt ===
        base_prompt = f"""
Tu es un analyste financier senior spécialisé en stratégie d'investissement.
Résume et interprète les résultats d'analyse du S&P500 ci-dessous.

Mode: {prompt_mode}
Langue: {language}

Analyse à interpréter :
{json.dumps(analysis_data, indent=2, ensure_ascii=False)}

Objectifs :
1. Résumer la situation globale du marché
2. Identifier les secteurs gagnants/perdants
3. Proposer une stratégie d'investissement claire et concise
4. Mettre en avant les risques principaux à surveiller
5. Terminer avec une conclusion professionnelle (ton analyste)
"""
        
        # === Call Bedrock ===
        response = bedrock.invoke_model(
            modelId="anthropic.claude-3-sonnet-20240229-v1:0",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [
                    {"role": "user", "content": base_prompt}
                ]
            })
        )
        logger.info("Model invoked successfully")
        
        # === Parse response ===
        body = json.loads(response["body"].read())
        text_out = body.get("content", [{}])[0].get("text", "")
        if not text_out:
            raise ValueError("Empty response from model")
        
        # === Append AI synthesis ===
        execution_time = round(time.time() - start_time, 2)
        ai_result = {
            "summary": "Synthèse stratégique générée avec succès",
            "recommendations": text_out.strip(),
            "metadata": {
                "model_used": "anthropic.claude-3-sonnet-20240229-v1:0",
                "prompt_mode": prompt_mode,
                "language": language,
                "execution_time_seconds": execution_time
            }
        }
        
        analysis_data["ai_synthesis"] = ai_result
        
        # === Save to DynamoDB ===
        _save_analysis_to_dynamodb(analysis_id, analysis_data)
        
        logger.info(f"Decision completed in {execution_time}s")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "analysis_id": analysis_id,
                "analysis": analysis_data,
                "saved_to_dynamodb": True
            }, ensure_ascii=False)
        }
    
    except ValueError as e:
        logger.warning(f"ValueError: {str(e)}")
        return _error_response(400, str(e))
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        return _error_response(500, str(e))


def _save_analysis_to_dynamodb(analysis_id: str, analysis_data: dict) -> None:
    """Save analysis to DynamoDB, converting floats to Decimal"""
    try:
        table = dynamodb.Table(ANALYSIS_TABLE)
        
        # Convert floats to Decimal for DynamoDB
        analysis_data = _convert_floats_to_decimal(analysis_data)
        
        item = {
            "analysis_id": analysis_id,
            "analysis": analysis_data,
            "created_at": datetime.now().isoformat(),
            "ttl": int(time.time()) + (7 * 24 * 3600)  # 7 days expiry
        }
        
        table.put_item(Item=item)
        logger.info(f"Analysis saved to DynamoDB: {analysis_id}")
        
    except Exception as e:
        logger.error(f"Failed to save to DynamoDB: {e}")
        raise


def _convert_floats_to_decimal(obj):
    """Recursively convert all floats to Decimal for DynamoDB compatibility"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: _convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_floats_to_decimal(i) for i in obj]
    return obj


def _error_response(code, message, details=None):
    return {
        "statusCode": code,
        "body": json.dumps({
            "error": {"code": code, "message": message, "details": details}
        }, ensure_ascii=False)
    }