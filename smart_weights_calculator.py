#!/usr/bin/env python3
import json
import os
import yfinance as yf
import boto3
from typing import Dict, List

def get_company_data(ticker: str) -> Dict:
    """Get company data from Yahoo Finance"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'business_summary': info.get('longBusinessSummary', ''),
            'market_cap': info.get('marketCap', 0),
            'revenue': info.get('totalRevenue', 0),
            'employees': info.get('fullTimeEmployees', 0)
        }
    except Exception as e:
        print(f"Error fetching Yahoo Finance data for {ticker}: {e}")
        return {}

def calculate_weights_with_llm(ticker: str, domain_tags: List[str], company_data: Dict) -> Dict[str, float]:
    """Use AWS Bedrock to calculate domain tag weights"""
    try:
        bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        prompt = f"""Analyze the company {ticker} and assign weights to these domain tags based on their business importance. The weights must sum to exactly 1.0.

Company Data:
- Sector: {company_data.get('sector', 'N/A')}
- Industry: {company_data.get('industry', 'N/A')}
- Business Summary: {company_data.get('business_summary', 'N/A')[:500]}...

Domain Tags: {', '.join(domain_tags)}

Return ONLY a JSON object with domain tags as keys and their weights as values. Weights must sum to 1.0.
Example format: {{"technology": 0.4, "consumer_electronics": 0.3, "mobile": 0.3}}"""

        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": prompt}]
        })

        response = bedrock.invoke_model(
            body=body,
            modelId='anthropic.claude-3-sonnet-20240229-v1:0',
            accept='application/json',
            contentType='application/json'
        )

        response_body = json.loads(response.get('body').read())
        content = response_body['content'][0]['text']
        
        # Extract JSON from response
        start = content.find('{')
        end = content.rfind('}') + 1
        weights_json = content[start:end]
        weights = json.loads(weights_json)
        
        # Normalize to ensure sum = 1.0
        total = sum(weights.values())
        if total > 0:
            weights = {k: v/total for k, v in weights.items()}
        
        return weights
        
    except Exception as e:
        print(f"Error with LLM calculation: {e}")
        # Fallback: equal weights
        weight = 1.0 / len(domain_tags)
        return {tag: weight for tag in domain_tags}

def process_json_file(file_path: str):
    """Process a single JSON file to add smart weights"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        ticker = data.get('ticker')
        domain_tags = data.get('domain_tags', [])
        
        if not ticker or not domain_tags:
            print(f"Skipping {file_path}: missing ticker or domain_tags")
            return
        
        print(f"Processing {ticker}...")
        
        # Get Yahoo Finance data
        company_data = get_company_data(ticker)
        
        # Calculate weights with LLM
        weights = calculate_weights_with_llm(ticker, domain_tags, company_data)
        
        # Add weights to JSON
        data['domain_tags_weights'] = weights
        
        # Save updated file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✓ Updated {ticker} with weights (sum: {sum(weights.values()):.3f})")
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    directory = "/home/sagemaker-user/shared/Project/advanced_tags/sector_classified_enhanced"
    
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            process_json_file(file_path)

if __name__ == "__main__":
    main()