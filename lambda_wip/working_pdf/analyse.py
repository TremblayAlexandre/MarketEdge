import json
import boto3
import base64
import uuid
import os
from typing import Dict, Tuple
from datetime import datetime

# Import the unified extraction and translation module
from unified_extract_and_translate import extract_and_translate, normalize_text

# Configuration
S3_RESULTS_BUCKET = os.getenv('S3_RESULTS_BUCKET', 'law-analysis-documents')
S3_RESULTS_PREFIX = 'law-analysis-results/'
SQS_QUEUE_URL = os.getenv('SQS_ANALYSIS_QUEUE_URL')


def lambda_handler(event, context):
    """
    Hybrid handler that processes both HTTP requests and SQS messages.
    
    HTTP Request Flow:
    1. Client submits → /api/analyse
    2. Create job_id and queue to SQS
    3. Return job_id immediately
    
    SQS Message Flow:
    1. SQS triggers Lambda with queued job
    2. Process analysis
    3. Save results to S3
    """
    
    # Check if this is an SQS event or HTTP request
    if 'Records' in event:
        # This is an SQS message
        return _handle_sqs_event(event, context)
    else:
        # This is an HTTP request
        return _handle_http_request(event, context)


def _handle_http_request(event: Dict, context) -> Dict:
    """
    Handle HTTP requests from API Gateway.
    Returns immediately with job_id, queues job to SQS.
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        print(f"[Analyse] New HTTP request job: {job_id}")
        
        # Validate input
        document_type = event.get('document_type', '').lower()
        if document_type not in ['txt', 'html', 'xml', 'pdf']:
            return _error_response(
                400,
                'Bad Request',
                f'Invalid document_type. Must be one of: txt, html, xml, pdf. Got: {document_type}'
            )
        
        # Validate we have document content or S3 reference
        has_content = bool(event.get('document_content') or event.get('document_binary'))
        has_s3 = bool(event.get('s3_bucket') and event.get('s3_key'))
        
        if not has_content and not has_s3:
            return _error_response(
                400,
                'Bad Request',
                'Must provide either document_content or (s3_bucket + s3_key)'
            )
        
        # Store initial job status in S3
        _save_job_status(job_id, 'queued', {
            'document_type': document_type,
            'submitted_at': datetime.utcnow().isoformat(),
            'law_title': event.get('law_title', 'Untitled'),
            'stage': 'waiting_in_queue',
            'estimated_wait_seconds': 60
        })
        
        print(f"[Analyse] Job {job_id} status saved to S3")
        
        # Queue to SQS
        if SQS_QUEUE_URL:
            _queue_to_sqs(job_id, event)
            print(f"[Analyse] Job {job_id} queued to SQS")
        else:
            print(f"[Analyse] WARNING: SQS_ANALYSIS_QUEUE_URL not set")
            return _error_response(
                500,
                'Internal Server Error',
                'SQS queue not configured'
            )
        
        # Return immediately with job_id
        response = {
            'statusCode': 202,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'job_id': job_id,
                'status': 'queued',
                'poll_url': f'/api/status/{job_id}',
                'message': 'Your analysis has been queued. Poll the status endpoint to check progress.',
                'estimate_seconds': 60
            })
        }
        
        print(f"[Analyse] Returning job_id: {job_id}")
        return response
        
    except ValueError as e:
        return _error_response(400, 'Bad Request', str(e))
    except Exception as e:
        print(f"[Analyse] ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return _error_response(500, 'Internal Server Error', f'Error queuing analysis: {str(e)}')


def _handle_sqs_event(event: Dict, context) -> Dict:
    """
    Handle SQS trigger events.
    Process the analysis and save results to S3.
    """
    print(f"[Worker] Received {len(event['Records'])} SQS message(s)")
    
    for record in event['Records']:
        try:
            # Parse SQS message (note: lowercase 'body', not 'Body')
            message_body = json.loads(record['body'])
            job_id = message_body['job_id']
            event_data = message_body['event']
            
            print(f"[Worker] Processing job: {job_id}")
            
            # Process the analysis
            _process_analysis(job_id, event_data, context)
            
            print(f"[Worker] Job {job_id} completed successfully")
            
        except Exception as e:
            print(f"[Worker] ERROR processing message: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Return 200 to acknowledge successful processing
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Messages processed'})
    }


def _process_analysis(job_id: str, event: Dict, context) -> None:
    """
    Process the analysis job.
    """
    try:
        document_type = event.get('document_type', '').lower()
        
        # Update status: starting extraction
        _save_job_status(job_id, 'processing', {
            'stage': 'extracting_document',
            'progress': '10%',
            'started_at': datetime.utcnow().isoformat()
        })
        
        # Extract document content
        extracted_text, extraction_method = _extract_document_content(event, document_type)
        
        if not extracted_text:
            _save_job_status(job_id, 'failed', {
                'error': 'Could not extract text from document',
                'stage': 'extraction_failed'
            })
            return
        
        print(f"[Worker] Job {job_id}: Extracted {len(extracted_text)} characters")
        
        # Update status: normalizing
        _save_job_status(job_id, 'processing', {
            'stage': 'normalizing_and_translating',
            'progress': '25%',
            'extracted_chars': len(extracted_text)
        })
        
        # Normalize and translate
        auto_translate = event.get('auto_translate', True)
        
        try:
            translation_result = extract_and_translate(
                document_content=extracted_text,
                document_type=document_type,
                auto_translate=auto_translate
            )
            
            analysis_text = translation_result['translated_text']
            translation_metadata = {
                'original_length': translation_result['metadata']['original_length'],
                'normalized_length': translation_result['metadata']['normalized_length'],
                'was_translated': translation_result['metadata']['was_translated'],
                'document_source_type': extraction_method
            }
            
        except Exception as e:
            print(f"[Worker] Job {job_id} WARNING: Translation/normalization failed: {str(e)}")
            analysis_text = extracted_text
            translation_metadata = {
                'original_length': len(extracted_text),
                'normalized_length': len(extracted_text),
                'was_translated': False,
                'document_source_type': extraction_method,
                'translation_error': str(e)
            }
        
        # Update status: analyzing with Bedrock
        _save_job_status(job_id, 'processing', {
            'stage': 'calling_bedrock_model',
            'progress': '40%'
        })
        
        # Call Bedrock for analysis
        bedrock = boto3.client('bedrock-runtime', region_name='us-west-2')
        tool_config = _get_tool_config()
        system_prompt = _get_system_prompt()
        
        user_message = [{
            "role": "user",
            "content": [{
                "text": f"""Analyze the following law document and provide a comprehensive economic impact assessment.

Document Format: {document_type.upper()}
Extraction Method: {extraction_method}
Text Normalized & Translated: {'Yes' if translation_metadata['was_translated'] else 'No'}

Law Document Content:
{analysis_text}

Provide:
1. A clear summary and confirm/correct the jurisdiction
2. Economic impact assessment for all relevant countries (focus on economic impacts)
3. Economic impact scores for affected sectors from the 11 standard categories (-1 to +1 scale)
4. Macro tags (broad themes) and micro tags (specific topics)
5. Classification into strong/moderate positive/negative categories with specific sectors and tags
6. Key findings, potential risks, and analyst commentary
7. Realistic confidence metrics

Important:
- Only include sectors that are actually affected (economic impact score != 0)
- Ensure tags are actionable and specific, but not too pointy
- Base confidence metrics on the quality and completeness of the law text"""
            }]
        }]
        
        response = bedrock.converse(
            modelId="global.anthropic.claude-haiku-4-5-20251001-v1:0",
            system=system_prompt,
            messages=user_message,
            toolConfig=tool_config,
            inferenceConfig={
                "maxTokens": 4096,
                "temperature": 0.2
            }
        )
        
        # Extract structured output
        content = response['output']['message']['content']
        tool_use = None
        for item in content:
            if 'toolUse' in item:
                tool_use = item['toolUse']
                break
        
        if not tool_use:
            _save_job_status(job_id, 'failed', {
                'error': 'No structured output received from model',
                'stage': 'model_analysis_failed'
            })
            return
        
        analysis_output = tool_use['input']
        
        # Build result - only the analysis output
        result = {
            'law_analysis_output': analysis_output
        }
        
        print(f"[Worker] Job {job_id}: Analysis complete, saving to S3")
        
        # Save completed result
        _save_job_status(job_id, 'completed', {
            'stage': 'complete',
            'progress': '100%',
            'completed_at': datetime.utcnow().isoformat()
        }, result)
        
        print(f"[Worker] Job {job_id}: Successfully saved results")
        
    except Exception as e:
        print(f"[Worker] Job {job_id} ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
        _save_job_status(job_id, 'failed', {
            'error': str(e),
            'traceback': traceback.format_exc(),
            'stage': 'processing_error'
        })


def _queue_to_sqs(job_id: str, event: Dict) -> None:
    """Queue the analysis job to SQS."""
    if not SQS_QUEUE_URL:
        print("[Queue] SQS_ANALYSIS_QUEUE_URL not set")
        return
    
    try:
        sqs = boto3.client('sqs', region_name='us-west-2')
        
        message_body = {
            'job_id': job_id,
            'event': event,
            'queued_at': datetime.utcnow().isoformat()
        }
        
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageAttributes={
                'JobId': {
                    'StringValue': job_id,
                    'DataType': 'String'
                },
                'DocumentType': {
                    'StringValue': event.get('document_type', 'unknown'),
                    'DataType': 'String'
                }
            }
        )
        
        print(f"[Queue] Message sent to SQS: {response['MessageId']}")
        
    except Exception as e:
        print(f"[Queue] ERROR sending to SQS: {str(e)}")
        _save_job_status(job_id, 'failed', {
            'error': f'Failed to queue job: {str(e)}',
            'stage': 'queue_failed'
        })
        raise


def get_job_status(job_id: str) -> Dict:
    """
    Retrieve the status and results of an analysis job.
    Called by router's /api/status/{job_id} endpoint.
    """
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
                    'job_id': job_id,
                    'message': f'No job found with ID: {job_id}'
                })
            }
    
    except Exception as e:
        print(f"[Status] Error retrieving job {job_id}: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Status retrieval error',
                'message': str(e)
            })
        }


def _extract_document_content(event: Dict, document_type: str) -> Tuple[str, str]:
    """Extract text content from various document formats."""
    if document_type == 'pdf':
        return _extract_pdf(event)
    elif document_type == 'html':
        return _extract_html(event)
    elif document_type == 'xml':
        return _extract_xml(event)
    elif document_type == 'txt':
        return _extract_txt(event)
    else:
        raise ValueError(f'Unsupported document type: {document_type}')


def _extract_txt(event: Dict) -> Tuple[str, str]:
    """Extract text from plain text file."""
    content = event.get('document_content', '')
    
    if not content:
        raise ValueError('document_content is required for txt format')
    
    try:
        if _is_base64(content):
            content = base64.b64decode(content).decode('utf-8')
    except Exception:
        pass
    
    return content, 'direct_text_extraction'


def _extract_html(event: Dict) -> Tuple[str, str]:
    """Extract text from HTML content."""
    content = event.get('document_content', '')
    
    if not content:
        raise ValueError('document_content is required for html format')
    
    if _is_base64(content):
        content = base64.b64decode(content).decode('utf-8')
    
    import re
    text = re.sub(r'<[^>]+>', '\n', content)
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text, 'html_tag_stripping'


def _extract_xml(event: Dict) -> Tuple[str, str]:
    """Extract text from XML content."""
    content = event.get('document_content', '')
    
    if not content:
        raise ValueError('document_content is required for xml format')
    
    if _is_base64(content):
        content = base64.b64decode(content).decode('utf-8')
    
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(content)
        text = _extract_text_from_xml_element(root)
    except ET.ParseError:
        import re
        text = re.sub(r'<[^>]+>', '\n', content)
    
    text = ' '.join(text.split())
    return text, 'xml_parsing'


def _extract_pdf(event: Dict) -> Tuple[str, str]:
    """Extract text from PDF using Amazon Textract."""
    s3_bucket = event.get('s3_bucket')
    s3_key = event.get('s3_key')
    
    if s3_bucket and s3_key:
        return _extract_pdf_from_s3(s3_bucket, s3_key)
    
    if 'document_binary' in event:
        return _extract_pdf_from_binary(event.get('document_binary'))
    
    document_content = event.get('document_content', '')
    if not document_content:
        raise ValueError('For PDF format, provide either s3_bucket+s3_key, document_binary, or base64 encoded document_content')
    
    return _extract_pdf_from_bytes(document_content)


def _extract_pdf_from_s3(bucket: str, key: str) -> Tuple[str, str]:
    """Extract text from PDF in S3 using Textract."""
    textract = boto3.client('textract', region_name='us-west-2')
    
    print(f"[Textract] Extracting from S3: s3://{bucket}/{key}")
    
    response = textract.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        }
    )
    
    job_id = response['JobId']
    extracted_text = _wait_for_textract_job(textract, job_id)
    return extracted_text, 'aws_textract_s3_async'


def _extract_pdf_from_binary(pdf_bytes: bytes) -> Tuple[str, str]:
    """Extract text from raw binary PDF bytes."""
    textract = boto3.client('textract', region_name='us-west-2')
    
    if isinstance(pdf_bytes, str):
        raise ValueError("Expected binary data, got string")
    
    print(f"[Textract] Processing {len(pdf_bytes)} bytes of binary PDF")
    
    response = textract.detect_document_text(
        Document={'Bytes': pdf_bytes}
    )
    
    extracted_text = _parse_textract_response(response)
    return extracted_text, 'aws_textract_binary'


def _extract_pdf_from_bytes(document_content: str) -> Tuple[str, str]:
    """Extract text from base64 encoded PDF."""
    textract = boto3.client('textract', region_name='us-west-2')
    
    pdf_bytes = base64.b64decode(document_content)
    print(f"[Textract] Decoded {len(pdf_bytes)} bytes from base64")
    
    response = textract.detect_document_text(
        Document={'Bytes': pdf_bytes}
    )
    
    extracted_text = _parse_textract_response(response)
    return extracted_text, 'aws_textract_base64'


def _wait_for_textract_job(textract_client, job_id: str, max_wait_seconds: int = 300) -> str:
    """Poll Textract job until completion."""
    import time
    
    start_time = time.time()
    attempt = 0
    
    while (time.time() - start_time) < max_wait_seconds:
        response = textract_client.get_document_text_detection(JobId=job_id)
        
        if response['JobStatus'] == 'SUCCEEDED':
            elapsed = time.time() - start_time
            print(f"[Textract] Job {job_id} completed in {elapsed:.1f}s")
            return _parse_textract_response(response)
        elif response['JobStatus'] == 'FAILED':
            raise ValueError(f'Textract job failed: {response.get("StatusMessage", "Unknown error")}')
        
        print(f"[Textract] Job status: {response['JobStatus']} (attempt {attempt + 1})")
        
        wait_time = min(0.5 * (1.5 ** attempt), 2.0)
        time.sleep(wait_time)
        attempt += 1
    
    raise ValueError(f'Textract job timeout after {max_wait_seconds} seconds')


def _parse_textract_response(response: Dict) -> str:
    """Parse Textract response and extract all text."""
    text_parts = []
    
    for item in response.get('Blocks', []):
        if item['BlockType'] == 'LINE':
            text_content = item.get('Text', '').strip()
            if text_content:
                text_parts.append(text_content)
    
    return '\n'.join(text_parts)


def _extract_text_from_xml_element(element) -> str:
    """Recursively extract text from XML element."""
    text_parts = []
    
    if element.text:
        text_parts.append(element.text.strip())
    
    for child in element:
        text_parts.append(_extract_text_from_xml_element(child))
        if child.tail:
            text_parts.append(child.tail.strip())
    
    return ' '.join([t for t in text_parts if t])


def _is_base64(s: str) -> bool:
    """Check if a string is base64 encoded."""
    try:
        if isinstance(s, str):
            s_bytes = bytes(s, 'utf-8')
        elif isinstance(s, bytes):
            s_bytes = s
        else:
            return False
        decoded = base64.b64decode(s_bytes, validate=True)
        return len(decoded) > 100
    except Exception:
        return False


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
        
        print(f"[S3] Saved job status for {job_id}: {status}")
        
    except Exception as e:
        print(f"[S3] Error saving status: {str(e)}")


def _get_tool_config() -> Dict:
    """Return Bedrock tool configuration for structured output."""
    return {
        "tools": [{
            "toolSpec": {
                "name": "record_law_analysis",
                "description": "Records the comprehensive law analysis with financial impact classifications",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "law_metadata": {
                                "type": "object",
                                "properties": {
                                    "summary": {
                                        "type": "string",
                                        "description": "Brief summary of the law's purpose and main provisions"
                                    },
                                    "jurisdiction": {
                                        "type": "string",
                                        "description": "The jurisdiction where this law applies"
                                    }
                                },
                                "required": ["summary", "jurisdiction"]
                            },
                            "impact": {
                                "type": "object",
                                "properties": {
                                    "countries_affected": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "country": {"type": "string"},
                                                "impact": {
                                                    "type": "number",
                                                    "minimum": -1,
                                                    "maximum": 1
                                                },
                                                "direction": {
                                                    "type": "string",
                                                    "enum": ["positive", "negative", "neutral"]
                                                }
                                            },
                                            "required": ["country", "impact", "direction"]
                                        }
                                    },
                                    "sectors": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "sector": {
                                                    "type": "string",
                                                    "enum": [
                                                        "Energy",
                                                        "Materials",
                                                        "Industrials",
                                                        "Consumer Discretionary",
                                                        "Consumer Staples",
                                                        "Health Care",
                                                        "Financials",
                                                        "Information Technology",
                                                        "Communication Services",
                                                        "Utilities",
                                                        "Real Estate"
                                                    ]
                                                },
                                                "impact": {
                                                    "type": "number",
                                                    "minimum": -1,
                                                    "maximum": 1
                                                }
                                            },
                                            "required": ["sector", "impact"]
                                        }
                                    },
                                    "related_tags_macro": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "related_tags_micro": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    }
                                },
                                "required": ["countries_affected", "sectors", "related_tags_macro", "related_tags_micro"]
                            },
                            "analysis_notes": {
                                "type": "object",
                                "properties": {
                                    "key_findings": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "potential_risks": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "analyst_comments": {
                                        "type": "string"
                                    }
                                },
                                "required": ["key_findings", "potential_risks", "analyst_comments"]
                            },
                            "confidence_metrics": {
                                "type": "object",
                                "properties": {
                                    "model_confidence": {
                                        "type": "number",
                                        "minimum": 0,
                                        "maximum": 1
                                    },
                                    "data_completeness": {
                                        "type": "number",
                                        "minimum": 0,
                                        "maximum": 1
                                    },
                                    "legal_text_similarity": {
                                        "type": "number",
                                        "minimum": 0,
                                        "maximum": 1
                                    },
                                    "explanability_score": {
                                        "type": "number",
                                        "minimum": 0,
                                        "maximum": 1
                                    }
                                },
                                "required": ["model_confidence", "data_completeness", "legal_text_similarity", "explanability_score"]
                            }
                        },
                        "required": ["law_metadata", "impact", "analysis_notes", "confidence_metrics"]
                    }
                }
            }
        }],
        "toolChoice": {"tool": {"name": "record_law_analysis"}}
    }


def _get_system_prompt() -> list:
    """Return the system prompt for law analysis."""
    return [{
        "text": """You are an expert legislative and economic impact analyst.

Your role is to:
- Analyze law documents and assess their economic and regulatory impacts
- Classify economic impacts on sectors using a scale from -1 (very negative) to +1 (very positive)
- Identify affected countries and the direction of economic impact
- Categorize economic impacts into strong/moderate positive/negative buckets
- Generate relevant macro and micro-level tags
- Provide key findings, risks, and analyst commentary
- Assign confidence metrics based on text quality and clarity

Guidelines:
- Use ONLY the 11 standard sector classifications provided in the schema
- Assign economic impact scores carefully: -1 to -0.5 is strong negative, -0.5 to -0.2 is moderate negative, -0.2 to 0.2 is neutral, 0.2 to 0.5 is moderate positive, 0.5 to 1 is strong positive
- ALWAYS include United-States in the countries_affected section (if they are not affected give them an economic impact score of 0)
- Only include major/developed economies
- Tags should be lowercase with underscores (e.g., 'carbon_tax', 'ev_incentives')
- Tags should not be composed of more than 3 words (e.g. fossil_fuel_optimization is not acceptable)
- Limit yourself to up to 15 tags for the related_tags_micro and up to 15 tags for the related_tags_macro
- Be realistic about confidence metrics - don't overstate certainty
- Consider both direct and indirect effects on sectors
- Think about supply chain impacts and cross-border effects"""
    }]


def _error_response(status_code: int, error: str, message: str) -> Dict:
    """Return a formatted error response."""
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'error': error,
            'message': message
        })
    }