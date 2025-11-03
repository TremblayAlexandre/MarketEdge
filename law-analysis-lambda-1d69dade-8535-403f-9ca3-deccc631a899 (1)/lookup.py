"""
lookup.py - Dynamic Regulatory Impact Stock Analysis (Async Pattern) - FIXED
Routes from: POST /api/lookup
Input: JSON with law_analysis_output (from enhance endpoint)
Output: Job ID for polling, results saved to S3

Uses SQS + async workers to avoid timeouts
"""

import json
import re
import logging
import boto3
import os
import uuid
from typing import List, Dict, Any, Tuple
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
DB_CONFIG = {
    'host': 'aurora-cluster.cluster-c56oo28y2teh.us-west-2.rds.amazonaws.com',
    'user': 'admin',
    'password': 'ChangeMe123!Strong',
    'database': 'stocks_db',
    'port': 3306,
    'connection_timeout': 30
}

BEDROCK_MODEL_ID = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
BEDROCK_REGION = "us-west-2"
S3_RESULTS_BUCKET = os.getenv('S3_RESULTS_BUCKET', 'law-analysis-documents')
S3_RESULTS_PREFIX = 'lookup-results/'
# Use the same analysis queue for lookup (low traffic environment)
SQS_QUEUE_URL = os.getenv('SQS_ANALYSIS_QUEUE_URL') or os.getenv('SQS_LOOKUP_QUEUE_URL')


def lambda_handler(event, context):
    """
    Main Lambda handler - routes HTTP requests or SQS events.
    HTTP: Queue job and return job_id immediately
    SQS: Process the analysis
    """
    
    # Check if this is an SQS event
    if 'Records' in event:
        return _handle_sqs_event(event, context)
    else:
        return _handle_http_request(event, context)


def _handle_http_request(event: Dict, context) -> Dict:
    """
    Handle HTTP requests from API Gateway.
    Queue job to SQS and return job_id immediately.
    
    FIXED: Enhanced validation and error reporting
    """
    try:
        job_id = str(uuid.uuid4())
        
        logger.info(f"[LOOKUP] New HTTP request job: {job_id}")
        logger.info(f"[LOOKUP] Event keys: {list(event.keys())}")
        
        # Validate input - IMPROVED with detailed error reporting
        impact_analysis = event.get('law_analysis_output')
        
        if not impact_analysis:
            logger.error(f"[LOOKUP] Job {job_id}: Missing law_analysis_output")
            logger.error(f"[LOOKUP] Job {job_id}: Event structure: {list(event.keys())}")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Missing law_analysis_output',
                    'message': 'Request must include law_analysis_output from /api/enhance',
                    'received_keys': list(event.keys()),
                    'expected_structure': {
                        'law_analysis_output': {
                            'law_metadata': {'summary': 'str', 'jurisdiction': 'str'},
                            'impact_classification': {'strong_positive': {}, 'strong_negative': {}},
                            'analysis_notes': {'key_findings': [], 'potential_risks': []}
                        }
                    }
                })
            }
        
        # Validate it's a dict
        if not isinstance(impact_analysis, dict):
            logger.error(f"[LOOKUP] Job {job_id}: law_analysis_output is not a dict, got {type(impact_analysis)}")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Invalid law_analysis_output type',
                    'message': 'law_analysis_output must be an object, got: ' + str(type(impact_analysis)),
                    'received_type': str(type(impact_analysis))
                })
            }
        
        # Validate required keys
        required_keys = ['law_metadata', 'impact', 'analysis_notes']
        missing_keys = [k for k in required_keys if k not in impact_analysis]
        if missing_keys:
            logger.error(f"[LOOKUP] Job {job_id}: Missing keys in law_analysis_output: {missing_keys}")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Invalid law_analysis_output structure',
                    'message': f'Missing required keys: {missing_keys}',
                    'received_keys': list(impact_analysis.keys())
                })
            }
        
        # Save initial job status with debug info
        _save_job_status(job_id, 'queued', {
            'document_type': 'law_analysis',
            'submitted_at': datetime.utcnow().isoformat(),
            'jurisdiction': impact_analysis.get('law_metadata', {}).get('jurisdiction', 'Unknown'),
            'stage': 'waiting_in_queue',
            'input_validation': 'passed',
            'impact_analysis_keys': list(impact_analysis.keys())
        })
        
        logger.info(f"[LOOKUP] Job {job_id}: Status saved to S3")
        
        # Queue to SQS
        if SQS_QUEUE_URL:
            try:
                _queue_to_sqs(job_id, event)
                logger.info(f"[LOOKUP] Job {job_id}: Successfully queued to SQS")
            except Exception as queue_error:
                logger.error(f"[LOOKUP] Job {job_id}: SQS queueing failed: {str(queue_error)}")
                _save_job_status(job_id, 'failed', {
                    'error': f'Failed to queue job: {str(queue_error)}',
                    'stage': 'queue_failed'
                })
                return {
                    'statusCode': 500,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'error': 'Failed to queue analysis',
                        'message': str(queue_error)
                    })
                }
        else:
            logger.error("[LOOKUP] SQS_ANALYSIS_QUEUE_URL and SQS_LOOKUP_QUEUE_URL not set")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Service not configured',
                    'message': 'SQS queue not configured'
                })
            }
        
        # Return immediately with job_id
        return {
            'statusCode': 202,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'job_id': job_id,
                'status': 'queued',
                'poll_url': f'/api/status/{job_id}',
                'message': 'Stock lookup analysis has been queued. Poll the status endpoint to check progress.',
                'estimate_seconds': 30
            })
        }
    
    except Exception as e:
        logger.error(f"[LOOKUP] HTTP handler error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }


def _handle_sqs_event(event: Dict, context) -> Dict:
    """
    Handle SQS trigger events for lookup jobs.
    Called by router when job_type == 'lookup'
    """
    logger.info(f"[WORKER] Received {len(event['Records'])} SQS message(s)")
    
    for record in event['Records']:
        try:
            message_body = json.loads(record['body'])
            job_id = message_body['job_id']
            job_type = message_body.get('job_type', 'unknown')
            event_data = message_body['event']
            
            logger.info(f"[WORKER] Processing job: {job_id} (type: {job_type})")
            
            # Process as lookup job
            _process_lookup(job_id, event_data, context)
            logger.info(f"[WORKER] Job {job_id}: Completed successfully")
        
        except KeyError as ke:
            logger.error(f"[WORKER] Error: Missing required key in message: {str(ke)}", exc_info=True)
        except json.JSONDecodeError as je:
            logger.error(f"[WORKER] Error: Invalid JSON in message body: {str(je)}", exc_info=True)
        except Exception as e:
            logger.error(f"[WORKER] Unexpected error processing message: {str(e)}", exc_info=True)
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Lookup messages processed'})
    }


def _process_lookup(job_id: str, event: Dict, context) -> None:
    """
    Process the stock lookup analysis.
    FIXED: Better error handling and validation
    """
    try:
        logger.info(f"[WORKER] Job {job_id}: Processing started")
        logger.info(f"[WORKER] Job {job_id}: Event keys: {list(event.keys())}")
        
        # Extract law_analysis_output from event
        impact_analysis = event.get('law_analysis_output')
        
        if not impact_analysis:
            logger.error(f"[WORKER] Job {job_id}: No law_analysis_output found in event")
            logger.error(f"[WORKER] Job {job_id}: Event keys: {list(event.keys())}")
            _save_job_status(job_id, 'failed', {
                'error': 'No law_analysis_output in event',
                'stage': 'invalid_input',
                'event_keys': list(event.keys())
            })
            return
        
        if not isinstance(impact_analysis, dict):
            logger.error(f"[WORKER] Job {job_id}: law_analysis_output is not a dict, got {type(impact_analysis)}")
            _save_job_status(job_id, 'failed', {
                'error': f'law_analysis_output must be dict, got {type(impact_analysis)}',
                'stage': 'invalid_input'
            })
            return
        
        logger.info(f"[WORKER] Job {job_id}: law_analysis_output keys: {list(impact_analysis.keys())}")
        
        # Update status: starting
        _save_job_status(job_id, 'processing', {
            'stage': 'extracting_tags',
            'progress': '10%',
            'started_at': datetime.utcnow().isoformat()
        })
        
        # STAGE 1: Extract tags
        logger.info(f"[WORKER] Job {job_id}: STAGE 1 - TAG EXTRACTION")
        try:
            filtered_tags, tag_impacts = extract_high_impact_tags(impact_analysis)
        except Exception as extract_error:
            logger.error(f"[WORKER] Job {job_id}: Tag extraction failed: {str(extract_error)}", exc_info=True)
            _save_job_status(job_id, 'failed', {
                'error': f'Tag extraction failed: {str(extract_error)}',
                'stage': 'tag_extraction_failed'
            })
            return
        
        if not filtered_tags:
            logger.warning(f"[WORKER] Job {job_id}: No tags extracted from analysis")
            _save_job_status(job_id, 'failed', {
                'error': 'No relevant tags found in law analysis',
                'stage': 'tag_extraction_failed',
                'impact_analysis_keys': list(impact_analysis.keys())
            })
            return
        
        logger.info(f"[WORKER] Job {job_id}: Found {len(filtered_tags)} high-impact tags")
        
        # Update status
        _save_job_status(job_id, 'processing', {
            'stage': 'querying_database',
            'progress': '25%',
            'tags_extracted': len(filtered_tags)
        })
        
        # STAGE 2: DB Exploration
        logger.info(f"[WORKER] Job {job_id}: STAGE 2 - DB EXPLORATION")
        try:
            candidates = smart_db_exploration(filtered_tags, tag_impacts, impact_analysis)
        except Exception as db_error:
            logger.error(f"[WORKER] Job {job_id}: Database exploration failed: {str(db_error)}", exc_info=True)
            _save_job_status(job_id, 'failed', {
                'error': f'Database error: {str(db_error)}',
                'stage': 'db_exploration_failed'
            })
            return
        
        if not candidates:
            logger.warning(f"[WORKER] Job {job_id}: No candidate companies found")
            _save_job_status(job_id, 'completed', {
                'stage': 'complete',
                'progress': '100%',
                'completed_at': datetime.utcnow().isoformat()
            }, {
                'companies': [],
                'message': 'No relevant companies found'
            })
            return
        
        logger.info(f"[WORKER] Job {job_id}: Found {len(candidates)} candidate companies")
        
        # Update status
        _save_job_status(job_id, 'processing', {
            'stage': 'analyzing_companies',
            'progress': '50%',
            'candidates_found': len(candidates)
        })
        
        # STAGE 3: Unified Analysis with Bedrock
        logger.info(f"[WORKER] Job {job_id}: STAGE 3 - UNIFIED ANALYSIS")
        try:
            analysis_results = unified_analysis(
                candidates,
                impact_analysis,
                filtered_tags
            )
        except Exception as analysis_error:
            logger.error(f"[WORKER] Job {job_id}: Unified analysis failed: {str(analysis_error)}", exc_info=True)
            _save_job_status(job_id, 'failed', {
                'error': f'Analysis failed: {str(analysis_error)}',
                'stage': 'unified_analysis_failed'
            })
            return
        
        if not analysis_results:
            logger.warning(f"[WORKER] Job {job_id}: No positions generated")
            _save_job_status(job_id, 'completed', {
                'stage': 'complete',
                'progress': '100%',
                'completed_at': datetime.utcnow().isoformat()
            }, {
                'companies': [],
                'message': 'No significant regulatory impact detected'
            })
            return
        
        logger.info(f"[WORKER] Job {job_id}: Generated {len(analysis_results)} positions")
        
        # Update status
        _save_job_status(job_id, 'processing', {
            'stage': 'verifying_results',
            'progress': '85%',
            'positions_generated': len(analysis_results)
        })
        
        # STAGE 4: Lightweight Verification
        logger.info(f"[WORKER] Job {job_id}: STAGE 4 - VERIFICATION")
        try:
            verified, stats = lightweight_verification(
                analysis_results,
                impact_analysis,
                candidates
            )
        except Exception as verify_error:
            logger.error(f"[WORKER] Job {job_id}: Verification failed: {str(verify_error)}", exc_info=True)
            # Don't fail the job, just use unverified results
            verified = analysis_results
            stats = {'verified': len(verified), 'issues': 0, 'error': str(verify_error)}
        
        logger.info(f"[WORKER] Job {job_id}: Verified {len(verified)} positions")
        
        # Build result - include original law_analysis_output + lookup results
        result = {
            'law_analysis_output': impact_analysis,  # Include original input
            'companies': verified,
            'metadata': {
                'jurisdiction': impact_analysis.get('law_metadata', {}).get('jurisdiction'),
                'extracted_tags': len(filtered_tags),
                'candidates_evaluated': len(candidates),
                'positions_generated': len(verified),
                'verification_stats': stats,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        # Save completed result
        _save_job_status(job_id, 'completed', {
            'stage': 'complete',
            'progress': '100%',
            'completed_at': datetime.utcnow().isoformat()
        }, result)
        
        logger.info(f"[WORKER] Job {job_id}: Successfully saved results")
    
    except Exception as e:
        logger.error(f"[WORKER] Job {job_id}: FATAL ERROR: {str(e)}", exc_info=True)
        _save_job_status(job_id, 'failed', {
            'error': str(e),
            'stage': 'processing_error'
        })

def extract_high_impact_tags(impact_analysis: Dict[str, Any]) -> Tuple[List[str], Dict]:
    """Extract high-impact tags dynamically from input."""
    
    logger.info("[TAG EXTRACTION] Starting...")
    logger.info(f"[TAG EXTRACTION] Input keys: {list(impact_analysis.keys())}")
    
    try:
        # YOUR JSON HAS 'impact', NOT 'impact_classification'
        impact_data = impact_analysis.get('impact', {})
        analysis_notes = impact_analysis.get('analysis_notes', {})
        key_findings = analysis_notes.get('key_findings', [])
        
        logger.info(f"[TAG EXTRACTION] impact keys: {list(impact_data.keys())}")
        logger.info(f"[TAG EXTRACTION] key_findings count: {len(key_findings)}")
    except (KeyError, TypeError) as e:
        logger.error(f"[TAG EXTRACTION] Invalid input structure: {e}")
        return [], {}
    
    # Extract tags from macro and micro tags (your JSON structure)
    all_tags = set()
    tag_impacts = {}
    
    macro_tags = impact_data.get('related_tags_macro', [])
    micro_tags = impact_data.get('related_tags_micro', [])
    
    all_tags.update(macro_tags)
    all_tags.update(micro_tags)
    
    # Default impact for tags (since your JSON doesn't assign per-tag impact)
    for tag in all_tags:
        tag_impacts[tag] = 0.5  # neutral default
    
    if not all_tags:
        logger.warning("[TAG EXTRACTION] No tags found in input")
        return [], {}
    
    logger.info(f"[TAG EXTRACTION] Found {len(all_tags)} total tags")
    
    # Filter by cross-reference with key findings
    findings_text = ' '.join(str(f) for f in key_findings).lower()
    
    tag_scores = {}
    for tag in all_tags:
        if isinstance(tag, str):
            keywords = tag.lower().replace('_', ' ').split()
            mention_count = sum(findings_text.count(kw) for kw in keywords if kw)
            tag_scores[tag] = mention_count
    
    # Sort by relevance and limit to ~20
    filtered_tags = sorted(
        all_tags,
        key=lambda t: tag_scores.get(t, 0),
        reverse=True
    )[:20]
    
    logger.info(f"[TAG EXTRACTION] ✓ Filtered to {len(filtered_tags)} high-impact tags")
    
    return filtered_tags, tag_impacts

def smart_db_exploration(
    filtered_tags: List[str],
    tag_impacts: Dict,
    impact_analysis: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Query Aurora DB for relevant companies."""
    
    logger.info("[DB EXPLORATION] Starting...")
    
    try:
        import mysql.connector
        
        logger.info("[DB] Connecting to Aurora...")
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            connection_timeout=DB_CONFIG['connection_timeout']
        )

        logger.info("[DB] Successfully connected to Aurora")

        cursor = connection.cursor(dictionary=True)

        # Extract sectors from impact data
        impact_data = impact_analysis.get('impact', {})
        sectors_list = impact_data.get('sectors', [])

        all_sectors = set()
        for sector_obj in sectors_list:
            if isinstance(sector_obj, dict) and 'sector' in sector_obj:
                all_sectors.add(sector_obj['sector'])

        logger.info(f"[DB] Found {len(all_sectors)} sectors: {list(all_sectors)}")
        
        all_companies = {}
        
        # Query by sectors
        if all_sectors:
            try:
                sector_placeholders = ','.join(['%s'] * len(all_sectors))
                query = f"""
                    SELECT symbol, sector_primary, sector_secondary, domain_tags
                    FROM stocks
                    WHERE sector_primary IN ({sector_placeholders})
                    LIMIT 100
                """
                cursor.execute(query, list(all_sectors))
                results = cursor.fetchall()
                
                logger.info(f"[DB] Sector query returned {len(results)} companies")
                
                for company in results:
                    ticker = company.get('symbol')
                    if ticker:
                        all_companies[ticker] = {
                            **company,
                            'relevance_score': 1.0
                        }
            
            except Exception as e:
                logger.warning(f"[DB] Sector query error: {e}")
        
        # Query by tags (if fewer than 25 companies found)
        if len(all_companies) < 25 and filtered_tags:
            try:
                for tag in filtered_tags[:10]:
                    query = f"""
                        SELECT symbol, sector_primary, sector_secondary, domain_tags
                        FROM stocks
                        WHERE JSON_CONTAINS(domain_tags, %s, '$')
                        LIMIT 50
                    """
                    cursor.execute(query, (json.dumps(tag),))
                    results = cursor.fetchall()
                    
                    for company in results:
                        ticker = company.get('symbol')
                        if ticker:
                            if ticker not in all_companies:
                                all_companies[ticker] = {
                                    **company,
                                    'relevance_score': 0.5
                                }
                            else:
                                all_companies[ticker]['relevance_score'] += 0.5
            
            except Exception as e:
                logger.warning(f"[DB] Tag query error: {e}")
        
        cursor.close()
        connection.close()
        logger.info("[DB] Disconnected")
        
        if not all_companies:
            logger.warning("[DB EXPLORATION] No companies found")
            return []
        
        # Sort by relevance and limit to 25
        sorted_companies = sorted(
            all_companies.values(),
            key=lambda c: c.get('relevance_score', 0),
            reverse=True
        )[:25]
        
        logger.info(f"[DB EXPLORATION] ✓ Final candidate set: {len(sorted_companies)} companies")
        
        return sorted_companies
    
    except Exception as e:
        logger.error(f"[DB EXPLORATION] Error: {e}", exc_info=True)
        return []


def unified_analysis(
    companies: List[Dict[str, Any]],
    impact_analysis: Dict[str, Any],
    filtered_tags: List[str]
) -> List[Dict[str, Any]]:
    """Unified analyst: single LLM call, returns positions with regulatory hooks."""
    
    logger.info(f"[UNIFIED ANALYSIS] Analyzing {len(companies)} companies...")
    
    try:
        bedrock_client = boto3.client('bedrock-runtime', region_name=BEDROCK_REGION)
        
        # Build analysis context
        law_metadata = impact_analysis.get('law_metadata', {})
        law_summary = law_metadata.get('summary', 'Unknown regulation')
        jurisdiction = law_metadata.get('jurisdiction', 'Unknown')
        
        analysis_notes = impact_analysis.get('analysis_notes', {})
        key_findings = analysis_notes.get('key_findings', [])[:5]
        potential_risks = analysis_notes.get('potential_risks', [])[:5]
        
        system_prompt = """You are an expert financial analyst specializing in regulatory impact assessment.

TASK: Analyze how regulations impact each company's investment position.

FOR EACH COMPANY, PROVIDE:
1. REGULATORY HOOK: Which specific aspect of regulation affects this company?
2. BUSINESS IMPACT: How does it change operations/costs/revenue?
3. FLAGGED ASSUMPTIONS: Any assumptions about company operations
4. CONFIDENCE LEVEL: high/medium/low based on data completeness

POSITION SCALE (only include if |position| >= 0.15):
- 0.5-1.0: Strong positive
- 0.15-0.5: Moderate positive
- -0.7 to -0.15: Moderate negative
- -1.0 to -0.5: Strong negative

Return ONLY valid JSON array. No markdown."""
        
        # Prepare company data
        company_data = []
        for comp in companies[:15]:
            domain_tags = comp.get('domain_tags', [])
            if isinstance(domain_tags, str):
                try:
                    domain_tags = json.loads(domain_tags)
                except:
                    domain_tags = []
            
            company_data.append({
                'ticker': comp.get('symbol'),
                'sector_primary': comp.get('sector_primary'),
                'sector_secondary': comp.get('sector_secondary'),
                'domain_tags': domain_tags[:5]
            })
        
        user_message = f"""Analyze these companies for regulatory impact:

JURISDICTION: {jurisdiction}
REGULATION: {law_summary}

KEY FINDINGS:
{chr(10).join('- ' + str(f)[:100] for f in key_findings)}

POTENTIAL RISKS:
{chr(10).join('- ' + str(r)[:100] for r in potential_risks)}

COMPANIES TO ANALYZE:
{json.dumps(company_data, indent=2)}

Return JSON array with: ticker, position (-1 to 1), confidence_level (0-1), reasoning, regulatory_hook, business_impact.
Only include |position| >= 0.15."""
        
        logger.info("[UNIFIED ANALYSIS] Calling Bedrock...")
        
        response = bedrock_client.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": system_prompt}],
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            inferenceConfig={
                "maxTokens": 3000,
                "temperature": 0.2
            }
        )
        
        # Parse response
        response_text = response['output']['message']['content'][0]['text']
        
        try:
            json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
            if json_match:
                analysis_data = json.loads(json_match.group(0))
            else:
                analysis_data = json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.error(f"[UNIFIED ANALYSIS] Failed to parse response: {e}")
            logger.error(f"[UNIFIED ANALYSIS] Raw response: {response_text[:500]}")
            return []
        
        # Filter and format
        results = []
        for item in analysis_data:
            try:
                position = float(item.get('position', 0))
                
                if abs(position) >= 0.3:
                    results.append({
                        'ticker': item.get('ticker'),
                        'position': position,
                        'confidence_level': float(item.get('confidence_level', 0.5)),
                        'reasoning': str(item.get('reasoning', ''))[:300],
                        'regulatory_hook': str(item.get('regulatory_hook', '')),
                        'business_impact': str(item.get('business_impact', ''))[:200]
                    })
            except (ValueError, TypeError) as e:
                logger.warning(f"[UNIFIED ANALYSIS] Error processing item: {e}")
                continue
        
        logger.info(f"[UNIFIED ANALYSIS] ✓ Generated {len(results)} positions")
        return results
    
    except Exception as e:
        logger.error(f"[UNIFIED ANALYSIS] Error: {e}", exc_info=True)
        return []


def lightweight_verification(
    analysis_results: List[Dict[str, Any]],
    impact_analysis: Dict[str, Any],
    company_data: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict]:
    """Lightweight verification: rule-based checks."""
    
    logger.info(f"[VERIFICATION] Verifying {len(analysis_results)} positions...")
    
    verified = []
    issue_count = 0
    
    for item in analysis_results:
        try:
            confidence = float(item.get('confidence_level', 0.5))
            if confidence < 0.2:
                item['flagged_for_review'] = True
                issue_count += 1
            
            verified.append(item)
        
        except Exception as e:
            logger.warning(f"[VERIFICATION] Error verifying item: {e}")
            verified.append(item)
    
    stats = {'verified': len(verified), 'issues': issue_count}
    logger.info(f"[VERIFICATION] ✓ Verified {len(verified)} positions ({issue_count} issues)")
    
    return verified, stats


def _queue_to_sqs(job_id: str, event: Dict) -> None:
    """Queue the lookup job to SQS."""
    if not SQS_QUEUE_URL:
        logger.warning("[QUEUE] SQS_ANALYSIS_QUEUE_URL not set")
        raise ValueError("SQS_QUEUE_URL not configured")
    
    try:
        sqs = boto3.client('sqs', region_name='us-west-2')
        
        message_body = {
            'job_id': job_id,
            'job_type': 'lookup',  # ← KEY FIX: Set job_type to 'lookup'
            'event': event,
            'queued_at': datetime.utcnow().isoformat()
        }
        
        message_json = json.dumps(message_body)
        logger.info(f"[QUEUE] Message size: {len(message_json.encode('utf-8')) / 1024:.1f}KB")
        
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=message_json,
            MessageAttributes={
                'JobId': {
                    'StringValue': job_id,
                    'DataType': 'String'
                },
                'JobType': {  # ← KEY FIX: Add JobType attribute
                    'StringValue': 'lookup',
                    'DataType': 'String'
                }
            }
        )
        
        logger.info(f"[QUEUE] Message sent to SQS: {response['MessageId']}")
    
    except Exception as e:
        logger.error(f"[QUEUE] Error: {str(e)}", exc_info=True)
        raise


def get_job_status(job_id: str) -> Dict:
    """Retrieve the status and results of a lookup job."""
    try:
        s3_bucket = os.getenv('S3_RESULTS_BUCKET', S3_RESULTS_BUCKET)
        s3 = boto3.client('s3', region_name='us-west-2')
        
        status_key = f"{S3_RESULTS_PREFIX}{job_id}/status.json"
        
        try:
            response = s3.get_object(Bucket=s3_bucket, Key=status_key)
            status_data = json.loads(response['Body'].read().decode('utf-8'))
            
            status = status_data.get('status', 'unknown')
            metadata = status_data.get('metadata', {})
            
            if status == 'completed':
                result_key = f"{S3_RESULTS_PREFIX}{job_id}/result.json"
                try:
                    result_response = s3.get_object(Bucket=s3_bucket, Key=result_key)
                    result_data = json.loads(result_response['Body'].read().decode('utf-8'))
                    
                    return {
                        'statusCode': 200,
                        'headers': {'Content-Type': 'application/json'},
                        'body': json.dumps({
                            'job_id': job_id,
                            'status': 'completed',
                            'result': result_data,
                            'metadata': metadata,
                            'completed_at': metadata.get('completed_at')
                        })
                    }
                except s3.exceptions.NoSuchKey:
                    return {
                        'statusCode': 200,
                        'headers': {'Content-Type': 'application/json'},
                        'body': json.dumps({
                            'job_id': job_id,
                            'status': 'completed',
                            'metadata': metadata
                        })
                    }
            
            elif status == 'failed':
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'job_id': job_id,
                        'status': 'failed',
                        'error': metadata.get('error', 'Unknown error'),
                        'metadata': metadata
                    })
                }
            
            else:
                return {
                    'statusCode': 202,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({
                        'job_id': job_id,
                        'status': status,
                        'metadata': metadata,
                        'poll_again_in_seconds': 5
                    })
                }
        
        except s3.exceptions.NoSuchKey:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Job not found',
                    'job_id': job_id
                })
            }
    
    except Exception as e:
        logger.error(f"[STATUS] Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Status retrieval error',
                'message': str(e)
            })
        }


def _save_job_status(job_id: str, status: str, metadata: Dict, result: Dict = None) -> None:
    """Save job status and optional result to S3."""
    s3_bucket = os.getenv('S3_RESULTS_BUCKET', S3_RESULTS_BUCKET)
    s3 = boto3.client('s3', region_name='us-west-2')
    
    try:
        status_data = {
            'job_id': job_id,
            'status': status,
            'metadata': metadata,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        status_key = f"{S3_RESULTS_PREFIX}{job_id}/status.json"
        s3.put_object(
            Bucket=s3_bucket,
            Key=status_key,
            Body=json.dumps(status_data),
            ContentType='application/json'
        )
        
        if result:
            result_key = f"{S3_RESULTS_PREFIX}{job_id}/result.json"
            s3.put_object(
                Bucket=s3_bucket,
                Key=result_key,
                Body=json.dumps(result),
                ContentType='application/json'
            )
        
        logger.info(f"[S3] Saved job {job_id}: {status}")
    
    except Exception as e:
        logger.error(f"[S3] Error saving status: {str(e)}")