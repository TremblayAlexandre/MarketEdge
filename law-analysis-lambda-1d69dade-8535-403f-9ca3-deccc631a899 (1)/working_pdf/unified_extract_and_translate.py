import json
import boto3
import base64
from typing import Dict, Tuple, Optional, Union, List
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration for Amazon Translate
AWS_REGION = "us-west-2"
SOURCE_LANG = "auto"  # Auto-detect source language
TARGET_LANG = "en"    # Target language: English
MAX_WORKERS = 10      # Parallel translation workers (Amazon Translate can handle more concurrent requests)

print(f"AWS_REGION={AWS_REGION}, SOURCE_LANG={SOURCE_LANG}, TARGET_LANG={TARGET_LANG}, MAX_WORKERS={MAX_WORKERS}")

# Amazon Translate client
translate_client = boto3.client("translate", region_name=AWS_REGION)


def normalize_text(text: str) -> str:
    """Normalize extracted text by cleaning up formatting issues.
    
    This function:
    - Removes extra whitespace and newlines
    - Fixes common encoding issues
    - Normalizes line breaks
    - Removes control characters
    - Fixes spacing around punctuation
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Remove control characters (except newlines and tabs)
    text = ''.join(ch for ch in text if ord(ch) >= 32 or ch in '\n\t')
    
    # Normalize line breaks - replace multiple newlines with single
    text = re.sub(r'\n\n+', '\n\n', text)
    
    # Remove extra spaces but preserve intentional spacing
    text = re.sub(r'[ \t]+', ' ', text)
    
    # Fix spacing around punctuation
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    text = re.sub(r'([.,!?;:])\s*([A-Z])', r'\1 \2', text)
    
    # Remove leading/trailing whitespace from each line
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    
    # Remove leading/trailing whitespace overall
    text = text.strip()
    
    return text


def is_english(text: str) -> bool:
    """Simple heuristic to detect if text is primarily in English.
    
    Checks for common English words and patterns.
    """
    if not text or len(text) < 50:
        return True  # Assume English for very short text
    
    # List of common English words
    common_english_words = {
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
        'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at',
        'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she',
        'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their',
        'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go',
        'me', 'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know',
        'take', 'people', 'into', 'year', 'your', 'good', 'some', 'could', 'them',
        'see', 'other', 'than', 'then', 'now', 'look', 'only', 'come', 'its', 'over',
        'think', 'also', 'back', 'after', 'use', 'two', 'how', 'our', 'work',
        'first', 'well', 'way', 'even', 'new', 'want', 'because', 'any', 'these',
        'give', 'day', 'most', 'us', 'is', 'was', 'are', 'been', 'being', 'has',
        'had', 'does', 'did', 'should', 'may', 'might', 'must', 'shall', 'should'
    }
    
    # Convert to lowercase and extract words
    words = re.findall(r'\b\w+\b', text.lower())
    
    if not words:
        return True
    
    # Count how many common English words appear
    english_word_count = sum(1 for word in words if word in common_english_words)
    english_ratio = english_word_count / len(words)
    
    # If more than 20% of words are common English words, assume it's English
    return english_ratio > 0.2


def chunk_text_by_sentences(text: str, max_bytes: int = 9000) -> List[str]:
    """Split text into chunks that fit within Amazon Translate's byte limit.
    
    Amazon Translate has a 10,000 byte limit. We use 9000 to be safe.
    Splits intelligently on sentence boundaries to preserve context.
    
    Args:
        text: Text to split
        max_bytes: Maximum bytes per chunk (default 9000)
    
    Returns:
        List of text chunks
    """
    if len(text.encode('utf-8')) <= max_bytes:
        return [text]
    
    chunks = []
    current_chunk = ""
    
    # Split by sentence boundaries (. or ! or ?)
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    for sentence in sentences:
        if not sentence.strip():
            continue
            
        test_chunk = current_chunk + sentence + " "
        
        if len(test_chunk.encode('utf-8')) > max_bytes:
            # Current chunk is full, save it
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = sentence + " "
        else:
            current_chunk = test_chunk
    
    # Add remaining chunk
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    # Fallback: if no chunks created, force split by size
    if not chunks:
        chunks = [text[i:i+2500] for i in range(0, len(text), 2500)]
    
    return chunks


def _translate_single_chunk(chunk: str, chunk_index: int, max_retries: int = 3, 
                           wait_sec: float = 0.5) -> Tuple[int, str, bool]:
    """Translate a single chunk of text.
    
    Args:
        chunk: Text chunk to translate
        chunk_index: Index of this chunk (for ordering results)
        max_retries: Number of retry attempts
        wait_sec: Base wait time between retries
    
    Returns:
        Tuple of (chunk_index, translated_text, success)
    """
    if not chunk or not chunk.strip():
        return chunk_index, chunk, True
    
    for attempt in range(1, max_retries + 1):
        try:
            response = translate_client.translate_text(
                Text=chunk,
                SourceLanguageCode=SOURCE_LANG,
                TargetLanguageCode=TARGET_LANG
            )
            return chunk_index, response['TranslatedText'], True
        
        except translate_client.exceptions.TextSizeLimitExceededException:
            print(f"[Translate] Chunk {chunk_index}: Size limit exceeded")
            return chunk_index, chunk, False
        
        except Exception as e:
            if attempt == max_retries:
                print(f"[Translate] Chunk {chunk_index}: Failed after {max_retries} retries - {str(e)}")
                return chunk_index, chunk, False
            
            # Exponential backoff
            sleep_time = wait_sec * (2 ** (attempt - 1))
            time.sleep(sleep_time)
    
    return chunk_index, chunk, False


def translate_text_parallel(text: str, max_retries: int = 3, max_workers: int = MAX_WORKERS) -> Tuple[str, bool]:
    """Translate text using parallel/threaded Amazon Translate API calls.
    
    This function significantly accelerates translation by:
    1. Splitting large text into chunks
    2. Translating multiple chunks concurrently using ThreadPoolExecutor
    3. Reassembling chunks in correct order
    
    Args:
        text: Text to translate
        max_retries: Number of retry attempts per chunk
        max_workers: Number of parallel threads
    
    Returns:
        Tuple of (translated_text, was_translated)
    """
    # Skip empty or whitespace-only text
    if not text or not text.strip():
        return text, False
    
    # Check if text is in English
    if is_english(text):
        print(f"[Translate] Text appears to be English, skipping translation")
        return text, False
    
    # Split into chunks
    chunks = chunk_text_by_sentences(text)
    
    if len(chunks) == 1:
        # Single chunk - no need for threading
        print(f"[Translate] Single chunk ({len(chunks[0].encode('utf-8'))} bytes), translating...")
        idx, translated, success = _translate_single_chunk(chunks[0], 0, max_retries)
        return translated, success
    
    # Multiple chunks - use parallel translation
    print(f"[Translate] Text split into {len(chunks)} chunks, starting parallel translation with {max_workers} workers...")
    start_time = time.time()
    
    translated_chunks = {}
    successful_chunks = 0
    failed_chunks = 0
    
    # Submit all chunks to thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_translate_single_chunk, chunk, i, max_retries): i 
            for i, chunk in enumerate(chunks)
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(futures):
            try:
                chunk_idx, translated_chunk, success = future.result()
                translated_chunks[chunk_idx] = translated_chunk
                completed += 1
                
                if success:
                    successful_chunks += 1
                else:
                    failed_chunks += 1
                
                # Progress update every 5 chunks or at end
                if completed % 5 == 0 or completed == len(chunks):
                    elapsed = time.time() - start_time
                    print(f"[Translate] Progress: {completed}/{len(chunks)} chunks done ({100*completed//len(chunks)}%) - {elapsed:.1f}s elapsed")
            
            except Exception as e:
                chunk_idx = futures[future]
                translated_chunks[chunk_idx] = chunks[chunk_idx]  # Keep original
                failed_chunks += 1
                print(f"[Translate] Chunk {chunk_idx}: Exception - {str(e)}")
    
    # Reassemble chunks in correct order
    translated_text = ' '.join([translated_chunks[i] for i in range(len(chunks))])
    
    elapsed = time.time() - start_time
    print(f"[Translate] Completed in {elapsed:.1f}s")
    print(f"[Translate]   Successful: {successful_chunks}/{len(chunks)}")
    if failed_chunks > 0:
        print(f"[Translate]   Failed: {failed_chunks}/{len(chunks)} (kept originals)")
    
    was_translated = successful_chunks > 0
    return translated_text, was_translated


def translate_text(text: str, max_retries: int = 3, wait_sec: float = 1.0) -> str:
    """Translate text using Amazon Translate if not in English.
    
    Uses parallel translation for better performance.
    
    Args:
        text: Text to translate
        max_retries: Number of retry attempts
        wait_sec: Base wait time between retries
    
    Returns:
        Translated text if not English, original text if already English or on error
    """
    # Use parallel translation
    translated_text, was_translated = translate_text_parallel(text, max_retries, MAX_WORKERS)
    return translated_text


# ============================================================================
# TEXT EXTRACTION FUNCTIONS
# ============================================================================

def _extract_document_content(document_type: str, extracted_text: str) -> Tuple[str, str]:
    """Normalize and prepare extracted text from any source.
    
    Args:
        document_type: Type of document (txt, html, xml, pdf)
        extracted_text: The raw extracted text
    
    Returns:
        Tuple of (normalized_text, description)
    """
    # Normalize the text
    normalized_text = normalize_text(extracted_text)
    
    # Add source-specific normalization if needed
    if document_type == 'html':
        # HTML text might have entity codes - handle them
        import html
        try:
            normalized_text = html.unescape(normalized_text)
        except:
            pass
    
    elif document_type == 'xml':
        # XML might have similar entity issues
        import html
        try:
            normalized_text = html.unescape(normalized_text)
        except:
            pass
    
    return normalized_text, document_type


def extract_and_translate(document_content: str, document_type: str, 
                          auto_translate: bool = True, max_workers: int = MAX_WORKERS) -> Dict:
    """
    Extract text from document and automatically translate if not English.
    
    Uses parallel translation for improved performance on large documents.
    
    Args:
        document_content: The document text (already extracted from PDF/HTML/XML)
        document_type: Type of document ('txt', 'html', 'xml', 'pdf')
        auto_translate: Whether to automatically translate if not English
        max_workers: Number of parallel workers for translation
    
    Returns:
        Dictionary with:
        - original_text: Raw extracted text
        - normalized_text: Cleaned and normalized text
        - translated_text: Translated to English (if applicable)
        - language_detected: Whether translation was performed
        - metadata: Additional metadata about the process
    """
    
    if not document_content:
        raise ValueError("Document content cannot be empty")
    
    if document_type not in ['txt', 'html', 'xml', 'pdf']:
        raise ValueError(f'Invalid document_type: {document_type}')
    
    print(f"\n{'='*70}")
    print(f"Processing {document_type.upper()} document")
    print(f"{'='*70}")
    
    start_time = time.time()
    
    # Step 1: Normalize the extracted text
    print(f"Step 1: Normalizing text...")
    normalized_text, doc_type = _extract_document_content(document_type, document_content)
    print(f"  Original length: {len(document_content)} chars")
    print(f"  Normalized length: {len(normalized_text)} chars")
    
    # Step 2: Detect language and translate if needed (with parallel acceleration)
    print(f"\nStep 2: Language detection and translation...")
    translated_text = normalized_text
    language_detected = False
    
    if auto_translate:
        if not is_english(normalized_text):
            language_detected = True
            print(f"  Non-English content detected, translating with parallel workers...")
            translated_text, translation_success = translate_text_parallel(
                normalized_text, 
                max_retries=3, 
                max_workers=max_workers
            )
            print(f"  Translated length: {len(translated_text)} chars")
        else:
            print(f"  English content detected, no translation needed")
    
    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"Processing complete! (Total time: {elapsed:.1f}s)")
    print(f"{'='*70}\n")
    
    return {
        'original_text': document_content,
        'normalized_text': normalized_text,
        'translated_text': translated_text,
        'language_detected': language_detected,
        'metadata': {
            'document_type': document_type,
            'original_length': len(document_content),
            'normalized_length': len(normalized_text),
            'translated_length': len(translated_text) if language_detected else len(normalized_text),
            'was_translated': language_detected,
            'processing_time_seconds': elapsed
        }
    }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Example 1: English text (no translation needed)
    english_text = """
    The United States Environmental Protection Agency (EPA) hereby establishes new regulations 
    regarding carbon emissions. These regulations apply to all industrial facilities with more than 
    10,000 tons of annual emissions. Companies must comply within 18 months of this act's passage.
    """
    
    print("\n" + "="*70)
    print("EXAMPLE 1: English Text")
    print("="*70)
    result = extract_and_translate(english_text, 'txt', auto_translate=True, max_workers=10)
    print(f"Was translated: {result['language_detected']}")
    print(f"Processing time: {result['metadata']['processing_time_seconds']:.2f}s")
    print(f"Normalized text preview: {result['normalized_text'][:100]}...\n")
    
    # Example 2: Non-English text (will be translated with parallel acceleration)
    spanish_text = """
    La Agencia de Protección Ambiental de los Estados Unidos (EPA) establece nuevas regulaciones 
    sobre emisiones de carbono. Estas regulaciones se aplican a todas las instalaciones industriales 
    con más de 10,000 toneladas de emisiones anuales. Las empresas deben cumplir dentro de 18 meses 
    de la aprobación de esta ley. Además, el EPA implementará un sistema de monitoreo continuo para 
    garantizar el cumplimiento. Las infracciones resultarán en multas sustanciales y posible cierre 
    de instalaciones. El objetivo es reducir las emisiones totales de carbono en un 40% dentro de 
    cinco años.
    """ * 5  # Repeat to create a larger document
    
    print("="*70)
    print("EXAMPLE 2: Spanish Text (Large Document - will use parallel translation)")
    print("="*70)
    result = extract_and_translate(spanish_text, 'txt', auto_translate=True, max_workers=10)
    print(f"Was translated: {result['language_detected']}")
    print(f"Processing time: {result['metadata']['processing_time_seconds']:.2f}s")
    print(f"Translated text preview: {result['translated_text'][:150]}...\n")