import json
import boto3
import base64
import os
from typing import Dict, Tuple
from datetime import datetime

# Import the unified extraction and translation module
from unified_extract_and_translate import extract_and_translate, normalize_text

# Configuration
S3_RESULTS_BUCKET = os.getenv('S3_RESULTS_BUCKET', 'law-analysis-documents')
S3_RESULTS_PREFIX = 'law-analysis-results/'


def lambda_handler(event, context):
    """
    SQS Worker Lambda - processes analysis jobs from queue.
    
    Triggered by: SQS queue (law-analysis-queue)
    
    For each message:
    1. Extract job_id and event data
    2. Process the analysis
    3. Save results to S3
    4. Update job status
    """
    
    print(f"[Worker] Received {len(event['Records'])} message(s)")
    
    for record in event['Records']:
        try:
            # Parse the SQS message
            message_body = json.loads(record['Body'])
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
            # Continue to next message instead of failing


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
        
        # Build result
        result = {
            'law_analysis_output': analysis_output,
            'metadata': {
                'document_type': document_type,
                'extraction_method': extraction_method,
                'content_length': len(extracted_text),
                'normalized_content_length': len(analysis_text),
                'model_used': 'claude-haiku-4-5-20251001-v1:0',
                'translation_info': translation_metadata
            },
            'usage': {
                'input_tokens': response['usage']['inputTokens'],
                'output_tokens': response['usage']['outputTokens']
            }
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
    """Poll Textract job until completion (worker has 15 minutes, so 5 minute timeout is safe)."""
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
    """Return Bedrock tool configuration."""
    return {
        "tools": [{
            "toolSpec": {
                "name": "record_law_analysis",
                "description": "Records the comprehensive law analysis",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "law_metadata": {
                                "type": "object",
                                "properties": {
                                    "summary": {"type": "string"},
                                    "jurisdiction": {"type": "string"}
                                },
                                "required": ["summary", "jurisdiction"]
                            },
                            "impact": {
                                "type": "object",
                                "properties": {
                                    "countries_affected": {"type": "array"},
                                    "sectors": {"type": "array"},
                                    "related_tags_macro": {"type": "array"},
                                    "related_tags_micro": {"type": "array"}
                                },
                                "required": ["countries_affected", "sectors", "related_tags_macro", "related_tags_micro"]
                            },
                            "analysis_notes": {
                                "type": "object",
                                "properties": {
                                    "key_findings": {"type": "array"},
                                    "potential_risks": {"type": "array"},
                                    "analyst_comments": {"type": "string"}
                                },
                                "required": ["key_findings", "potential_risks", "analyst_comments"]
                            },
                            "confidence_metrics": {
                                "type": "object",
                                "properties": {
                                    "model_confidence": {"type": "number"},
                                    "data_completeness": {"type": "number"},
                                    "legal_text_similarity": {"type": "number"},
                                    "explanability_score": {"type": "number"}
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
    """Return system prompt for analysis."""
    return [{
        "text": """You are an expert legislative and economic impact analyst.

Guidelines:
- Use the 11 standard sector classifications provided in the schema
- Assign scores: -1 to -0.5 strong negative, -0.5 to -0.2 moderate negative, -0.2 to 0.2 neutral, 0.2 to 0.5 moderate positive, 0.5 to 1 strong positive
- ALWAYS include United-States in countries_affected
- Only include major/developed economies
- Tags lowercase with underscores, max 3 words each
- Max 15 tags each for macro and micro
- Be realistic about confidence metrics
- Consider direct and indirect effects, supply chain impacts"""
    }]