import json
import logging
import boto3
import os
from typing import Dict, Any, List, Set
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Initialize Comprehend client
comprehend = boto3.client('comprehend')

# Global variable to cache loaded tags
_CACHED_DOMAIN_TAGS = None


def unwrap_api_gateway_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Unwrap API Gateway event structure to get the actual payload.
    
    API Gateway wraps the payload in:
    {
        "body": "{...json string...}",
        "headers": {...},
        "isBase64Encoded": false
    }
    
    This function extracts and parses the actual payload.
    """
    logger.info("=" * 80)
    logger.info("UNWRAPPING API GATEWAY EVENT")
    logger.info("=" * 80)
    
    # Check if this is an API Gateway event
    if 'body' in event and isinstance(event.get('body'), str):
        logger.info("✓ Detected API Gateway wrapped event")
        logger.debug(f"Event is from API Gateway (has 'body' field)")
        
        try:
            # Parse the body
            body_str = event['body']
            logger.debug(f"Body type: {type(body_str)}, length: {len(body_str)}")
            
            actual_payload = json.loads(body_str)
            logger.info(f"✓ Successfully parsed body as JSON")
            logger.debug(f"Payload keys: {list(actual_payload.keys())}")
            
            return actual_payload
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse body as JSON: {str(e)}")
            raise
    else:
        logger.info("Direct event (not API Gateway wrapped)")
        return event


def unwrap_double_nesting(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle double-nested law_analysis_output structure.
    
    Sometimes the data comes as:
    {
        "law_analysis_output": {
            "law_analysis_output": {
                "impact": {...}
            }
        }
    }
    
    This function flattens it to:
    {
        "law_analysis_output": {
            "impact": {...}
        }
    }
    """
    logger.info("Checking for double-nested structure...")
    
    if 'law_analysis_output' in event:
        outer = event['law_analysis_output']
        logger.debug(f"Outer law_analysis_output keys: {list(outer.keys())}")
        
        # Check if inner structure is also law_analysis_output
        if isinstance(outer, dict) and 'law_analysis_output' in outer:
            logger.warning("⚠ Detected double-nested law_analysis_output!")
            inner = outer['law_analysis_output']
            logger.info(f"✓ Unwrapping inner structure")
            logger.debug(f"Inner structure keys: {list(inner.keys())}")
            
            # Return the inner structure
            return {**event, 'law_analysis_output': inner}
    
    return event


def load_domain_tags() -> List[str]:
    """
    Load domain tags from compiled_domain_tags.json.
    Uses caching to avoid repeated file reads during function warm starts.
    """
    global _CACHED_DOMAIN_TAGS
    
    if _CACHED_DOMAIN_TAGS is not None:
        logger.info(f"Using cached domain tags ({len(_CACHED_DOMAIN_TAGS)} tags)")
        return _CACHED_DOMAIN_TAGS
    
    possible_paths = [
        '/var/task/compiled_domain_tags.json',
        'compiled_domain_tags.json',
        '/opt/compiled_domain_tags.json',
        os.path.join(os.path.dirname(__file__), 'compiled_domain_tags.json')
    ]
    
    tags_file = None
    for path in possible_paths:
        logger.debug(f"Checking path: {path}")
        if os.path.exists(path):
            tags_file = path
            logger.info(f"✓ Found domain tags file at: {tags_file}")
            break
    
    if not tags_file:
        logger.error(f"✗ compiled_domain_tags.json not found in: {possible_paths}")
        raise FileNotFoundError(f"compiled_domain_tags.json not found in: {possible_paths}")
    
    try:
        with open(tags_file, 'r') as f:
            _CACHED_DOMAIN_TAGS = json.load(f)
        
        if not isinstance(_CACHED_DOMAIN_TAGS, list):
            raise ValueError("compiled_domain_tags.json must contain a JSON array")
        
        logger.info(f"✓ Successfully loaded {len(_CACHED_DOMAIN_TAGS)} domain tags")
        return _CACHED_DOMAIN_TAGS
        
    except json.JSONDecodeError as e:
        logger.error(f"✗ Invalid JSON in compiled_domain_tags.json: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"✗ Error loading domain tags: {str(e)}")
        raise


def debug_impact_structure(impact: Dict[str, Any]) -> None:
    """Debug function to trace impact structure issues."""
    logger.debug("=" * 80)
    logger.debug("IMPACT STRUCTURE DEBUG")
    logger.debug("=" * 80)
    logger.debug(f"Impact keys: {list(impact.keys())}")
    
    sectors = impact.get('sectors', [])
    logger.debug(f"Sectors: {len(sectors) if sectors else 0}")
    if sectors:
        logger.debug(f"  Sample sector: {sectors[0] if sectors else 'N/A'}")
    else:
        logger.warning("⚠ NO SECTORS FOUND")
    
    macro_tags = impact.get('related_tags_macro', [])
    logger.debug(f"Macro tags: {len(macro_tags) if macro_tags else 0}")
    if macro_tags:
        logger.debug(f"  Macro tags: {macro_tags}")
    else:
        logger.warning("⚠ NO MACRO TAGS FOUND")
    
    micro_tags = impact.get('related_tags_micro', [])
    logger.debug(f"Micro tags: {len(micro_tags) if micro_tags else 0}")
    if micro_tags:
        logger.debug(f"  Micro tags: {micro_tags}")
    else:
        logger.warning("⚠ NO MICRO TAGS FOUND")
    
    logger.debug("=" * 80)


def lambda_handler(event, context):
    """
    Enhanced law analysis enhancer with improved error tracing and API Gateway support.
    """
    
    try:
        logger.info("=" * 80)
        logger.info("LAMBDA HANDLER INVOKED")
        logger.info("=" * 80)
        
        # Step 1: Unwrap API Gateway event if needed
        logger.info("\n[STEP 1] Unwrapping API Gateway event...")
        try:
            payload = unwrap_api_gateway_event(event)
            logger.info("✓ Event unwrapped successfully")
        except Exception as e:
            logger.error(f"✗ Failed to unwrap event: {str(e)}", exc_info=True)
            return error_response(400, f"Failed to parse event: {str(e)}")
        
        # Step 2: Handle double nesting
        logger.info("\n[STEP 2] Checking for double nesting...")
        try:
            payload = unwrap_double_nesting(payload)
            logger.info("✓ Nesting structure normalized")
        except Exception as e:
            logger.error(f"✗ Failed to unwrap nesting: {str(e)}", exc_info=True)
        
        # Step 3: Load domain tags
        logger.info("\n[STEP 3] Loading domain tags...")
        try:
            domain_tags = load_domain_tags()
            logger.info(f"✓ Loaded {len(domain_tags)} domain tags")
        except Exception as e:
            logger.error(f"✗ Failed to load domain tags: {str(e)}", exc_info=True)
            return error_response(500, f"Failed to load domain tags: {str(e)}")
        
        # Step 4: Extract and validate law_analysis_output
        logger.info("\n[STEP 4] Extracting law_analysis_output...")
        law_analysis_output = payload.get('law_analysis_output')
        
        if not law_analysis_output:
            logger.error("✗ law_analysis_output not found in payload")
            logger.error(f"Available keys in payload: {list(payload.keys())}")
            return error_response(400, "law_analysis_output is required in payload")
        
        logger.info("✓ law_analysis_output found")
        logger.debug(f"law_analysis_output keys: {list(law_analysis_output.keys())}")
        
        # Step 5: Extract and validate impact data
        logger.info("\n[STEP 5] Extracting impact data...")
        impact = law_analysis_output.get('impact', {})
        
        if not impact:
            logger.error("✗ No 'impact' key found in law_analysis_output")
            logger.error(f"Available keys: {list(law_analysis_output.keys())}")
            return error_response(400, "impact data is required in law_analysis_output")
        
        logger.info("✓ impact data found")
        
        # Debug impact structure
        debug_impact_structure(impact)
        
        sectors = impact.get('sectors', []) or []
        macro_tags = impact.get('related_tags_macro', []) or []
        micro_tags = impact.get('related_tags_micro', []) or []
        
        logger.info(f"✓ Found {len(sectors)} sectors, {len(macro_tags)} macro tags, {len(micro_tags)} micro tags")
        
        if not sectors and not macro_tags and not micro_tags:
            logger.warning("⚠ WARNING: No data to process!")
            logger.warning("This will result in 0 tags in the output")
        
        # Step 6: Extract options
        logger.info("\n[STEP 6] Extracting options...")
        allowed_tags = payload.get('allowed_tags', domain_tags)
        enable_comprehend = payload.get('enable_comprehend', True)
        text_to_analyze = payload.get('text_to_analyze', '')
        
        logger.info(f"✓ enable_comprehend: {enable_comprehend}")
        logger.info(f"✓ text_to_analyze length: {len(text_to_analyze)} chars")
        
        # Step 7: Classify impact
        logger.info("\n[STEP 7] Classifying impact...")
        thresholds = {
            'strong_pos': 0.6,
            'moderate_pos': 0.2,
            'moderate_neg': -0.2,
            'strong_neg': -0.6,
        }
        
        impact_classification = classify_impact(law_analysis_output, allowed_tags, thresholds)
        logger.info("✓ Impact classification complete")
        
        # Step 8: Perform Comprehend analysis if enabled
        logger.info("\n[STEP 8] Processing Comprehend...")
        comprehend_insights = {}
        if enable_comprehend and text_to_analyze:
            logger.info("Starting Amazon Comprehend analysis")
            try:
                comprehend_insights = perform_comprehend_analysis(text_to_analyze)
                logger.info("✓ Comprehend analysis complete")
            except Exception as e:
                logger.warning(f"⚠ Comprehend analysis failed: {str(e)}", exc_info=True)
                comprehend_insights = {'error': str(e)}
        else:
            if not enable_comprehend:
                logger.info("Comprehend analysis disabled")
            if not text_to_analyze:
                logger.info("No text provided for Comprehend analysis")
        
        # Step 9: Build response
        logger.info("\n[STEP 9] Building response...")
        response_data = {
            **payload,
            'impact_classification': impact_classification,
            'comprehend_insights': comprehend_insights,
            'tags_source': 'compiled_domain_tags.json',
            'tags_count': len(allowed_tags),
            'debug_info': {
                'sectors_found': len(sectors),
                'macro_tags_found': len(macro_tags),
                'micro_tags_found': len(micro_tags),
                'total_candidate_tags': len(macro_tags) + len(micro_tags),
                'processing_successful': True
            }
        }
        
        logger.info("✓ Enhancement complete")
        logger.info("=" * 80)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        logger.error(f"✗ Unhandled error: {str(e)}", exc_info=True)
        logger.error(f"Exception type: {type(e).__name__}")
        return error_response(500, f"Unhandled error: {str(e)}")


def error_response(status_code: int, message: str) -> Dict[str, Any]:
    """Helper to create error responses."""
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'error': f'Error {status_code}',
            'message': message,
            'trace': 'Check CloudWatch logs for details'
        })
    }


def perform_comprehend_analysis(text: str) -> Dict[str, Any]:
    """Perform comprehensive Amazon Comprehend analysis."""
    try:
        if not text or len(text.strip()) == 0:
            logger.warning("Empty text provided for Comprehend analysis")
            return {}
        
        text_to_process = text[:5000]
        results = {}
        
        # Sentiment Analysis
        logger.info("Analyzing sentiment...")
        sentiment_response = comprehend.detect_sentiment(
            Text=text_to_process,
            LanguageCode='en'
        )
        results['sentiment'] = {
            'sentiment': sentiment_response.get('Sentiment'),
            'confidence_scores': sentiment_response.get('SentimentScore', {}),
            'mixed': sentiment_response.get('Sentiment') == 'MIXED'
        }
        logger.info(f"✓ Sentiment: {results['sentiment']['sentiment']}")
        
        # Key Phrases
        logger.info("Extracting key phrases...")
        key_phrases_response = comprehend.detect_key_phrases(
            Text=text_to_process,
            LanguageCode='en'
        )
        results['key_phrases'] = []
        for phrase in key_phrases_response.get('KeyPhrases', []):
            results['key_phrases'].append({
                'text': phrase.get('Text'),
                'score': phrase.get('Score'),
            })
        logger.info(f"✓ Extracted {len(results['key_phrases'])} key phrases")
        
        # Entities
        logger.info("Extracting entities...")
        entities_response = comprehend.detect_entities(
            Text=text_to_process,
            LanguageCode='en'
        )
        results['entities'] = {}
        for entity in entities_response.get('Entities', []):
            entity_type = entity.get('Type')
            if entity_type not in results['entities']:
                results['entities'][entity_type] = []
            results['entities'][entity_type].append({
                'text': entity.get('Text'),
                'score': entity.get('Score'),
            })
        logger.info(f"✓ Extracted {sum(len(v) for v in results['entities'].values())} entities")
        
        # Language
        logger.info("Detecting language...")
        language_response = comprehend.detect_dominant_language(Text=text_to_process)
        languages = language_response.get('Languages', [])
        if languages:
            results['language'] = {
                'language_code': languages[0].get('LanguageCode'),
                'score': languages[0].get('Score')
            }
        
        return results
        
    except ClientError as e:
        logger.error(f"✗ AWS Comprehend error: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"✗ Comprehend analysis error: {str(e)}", exc_info=True)
        raise


def bucket_for_value(v: float, thresholds: Dict[str, float]) -> str | None:
    """Classify impact value into bucket."""
    if v is None:
        return None
    if v >= thresholds['strong_pos']:
        return 'strong_positive'
    if v >= thresholds['moderate_pos']:
        return 'moderate_positive'
    if v <= thresholds['strong_neg']:
        return 'strong_negative'
    if v <= thresholds['moderate_neg']:
        return 'moderate_negative'
    return None


def normalize_token(s: str) -> str:
    """Normalize string for comparison."""
    return s.lower().replace('-', '_').replace(' ', '_')


def match_tags(candidate: str, allowed_tags: List[str]) -> List[str]:
    """Find allowed tags matching the candidate."""
    cand = normalize_token(candidate)
    cand_tokens = set([t for t in cand.split('_') if t])
    matches: Set[str] = set()
    
    for a in allowed_tags:
        a_norm = normalize_token(a)
        a_tokens = set([t for t in a_norm.split('_') if t])
        
        if a_norm in cand or cand in a_norm:
            matches.add(a)
            continue
        
        if cand_tokens & a_tokens:
            matches.add(a)
            continue
    
    return sorted(matches)


def estimate_tag_impact(tag: str, sectors: List[Dict[str, Any]]) -> float | None:
    """Estimate tag impact from sectors."""
    t = normalize_token(tag)
    tokens = set([p for p in t.split('_') if p])
    matched_values = []
    
    for s in sectors:
        s_name = normalize_token(s.get('sector', ''))
        s_tokens = set([p for p in s_name.split('_') if p])
        
        if tokens & s_tokens:
            try:
                matched_values.append(float(s.get('impact', 0.0)))
            except Exception:
                continue
    
    if matched_values:
        return sum(matched_values) / len(matched_values)
    
    values = []
    for s in sectors:
        try:
            values.append(float(s.get('impact', 0.0)))
        except Exception:
            continue
    
    if values:
        return sum(values) / len(values)
    
    return None


def classify_impact(
    response: Dict[str, Any],
    allowed_tags: List[str],
    thresholds: Dict[str, float] = None
) -> Dict[str, Any]:
    """Classify law analysis impact into categories."""
    
    if thresholds is None:
        thresholds = {
            'strong_pos': 0.6,
            'moderate_pos': 0.2,
            'moderate_neg': -0.2,
            'strong_neg': -0.6,
        }
    
    out = {
        'strong_positive': {'sectors': [], 'tags': []},
        'moderate_positive': {'sectors': [], 'tags': []},
        'moderate_negative': {'sectors': [], 'tags': []},
        'strong_negative': {'sectors': [], 'tags': []},
    }
    
    impact_block = response.get('impact', {})
    sectors = impact_block.get('sectors', []) or []
    macro_tags = impact_block.get('related_tags_macro', []) or []
    micro_tags = impact_block.get('related_tags_micro', []) or []
    candidate_tags = list(dict.fromkeys(macro_tags + micro_tags))
    
    logger.info(f"Classifying: {len(sectors)} sectors, {len(candidate_tags)} candidate tags")
    
    # Classify sectors
    for s in sectors:
        name = s.get('sector') or s.get('name') or ''
        try:
            val = float(s.get('impact'))
        except Exception:
            logger.warning(f"Could not parse impact for sector {name}")
            continue
        
        bucket = bucket_for_value(val, thresholds)
        if bucket:
            out[bucket]['sectors'].append(name)
            logger.debug(f"Sector '{name}' → {bucket}")
    
    # Classify tags
    matched_allowed_for_candidate: Dict[str, List[str]] = {}
    
    for c in candidate_tags:
        matches = match_tags(c, allowed_tags) if allowed_tags else []
        if matches:
            matched_allowed_for_candidate[c] = matches
            logger.debug(f"Tag '{c}' → {matches[:3]}")
    
    logger.info(f"Matched {len(matched_allowed_for_candidate)} candidate tags")
    
    tag_buckets: Dict[str, Set[str]] = {k: set() for k in out.keys()}
    
    for c, allowed_matches in matched_allowed_for_candidate.items():
        for a in allowed_matches:
            est = estimate_tag_impact(c, sectors)
            if est is None:
                continue
            
            bucket = bucket_for_value(est, thresholds)
            if bucket:
                tag_buckets[bucket].add(a)
    
    for bucket, data in out.items():
        data['tags'] = sorted(list(tag_buckets[bucket]))
        data['sectors'] = sorted(list(set(data['sectors'])))
    
    logger.info(f"Classification complete: {sum(len(data['sectors']) + len(data['tags']) for data in out.values())} items classified")
    
    return out