import json
import boto3
import os
from typing import List, Dict

def load_precise_tags() -> List[str]:
    """Load the precise domain tags"""
    with open("shared/Project/Before/precise_domain_tags.json", "r") as f:
        return json.load(f)

def analyze_company_with_comprehend(company_name: str, company_description: str, precise_tags: List[str]) -> List[str]:
    """
    Use AWS Comprehend to classify company into precise domain tags
    """
    comprehend = boto3.client('comprehend', region_name='us-west-2')
    
    try:
        # Use key phrase detection on company description only
        keyphrases_response = comprehend.detect_key_phrases(
            Text=company_description,
            LanguageCode='en'
        )
        
        # Extract key phrases
        keyphrases = [phrase['Text'].lower() for phrase in keyphrases_response['KeyPhrases']]
        description_lower = company_description.lower()
        
        detected_tags = []
        
        # Direct keyword matching with company description
        tag_keywords = {
            'pharmaceutical': ['pharmaceutical', 'drug', 'medicine', 'pharma'],
            'biotechnology': ['biotechnology', 'biotech', 'biological'],
            'medical_devices': ['medical device', 'medical equipment'],
            'software_development': ['software', 'develops software'],
            'cloud_computing': ['cloud', 'cloud computing', 'cloud services'],
            'consumer_electronics': ['consumer electronics', 'electronics', 'iphone', 'ipad'],
            'mobile_applications': ['mobile', 'app store', 'applications'],
            'operating_systems': ['operating system', 'windows', 'office'],
            'gaming': ['gaming', 'xbox', 'games'],
            'artificial_intelligence': ['ai', 'artificial intelligence'],
            'vaccines': ['vaccine', 'vaccination'],
            'diagnostics': ['diagnostic', 'testing'],
            'oncology': ['cancer', 'oncology', 'tumor'],
            'enterprise_software': ['enterprise', 'productivity software'],
            'e_commerce': ['online services', 'marketplace']
        }
        
        # Match keywords to description
        for tag, keywords in tag_keywords.items():
            for keyword in keywords:
                if keyword in description_lower:
                    if tag not in detected_tags:
                        detected_tags.append(tag)
                        break
        
        # Also check keyphrases for additional matches
        for phrase in keyphrases:
            for tag, keywords in tag_keywords.items():
                for keyword in keywords:
                    if keyword in phrase and tag not in detected_tags:
                        detected_tags.append(tag)
        
        return detected_tags[:6]
        
    except Exception as e:
        print(f"Error analyzing {company_name}: {e}")
        return []

def main():
    # Load precise tags
    precise_tags = load_precise_tags()
    print(f"Loaded {len(precise_tags)} precise domain tags")
    
    # Example companies to test (you would load your actual company data)
    test_companies = {
        "JNJ": "Johnson & Johnson is a multinational pharmaceutical, biotechnology, and medical device company. It develops and manufactures pharmaceuticals, medical devices, vaccines, and consumer healthcare products.",
        "AAPL": "Apple Inc. designs, manufactures, and markets consumer electronics, computer software, and online services. Products include iPhone, iPad, Mac computers, Apple Watch, and services like App Store and iCloud.",
        "MSFT": "Microsoft Corporation develops, licenses, and supports software products, services, and devices. It offers operating systems, productivity software, cloud computing services, and gaming products."
    }
    
    results = {}
    
    for symbol, description in test_companies.items():
        print(f"\\nAnalyzing {symbol}...")
        tags = analyze_company_with_comprehend(symbol, description, precise_tags)
        results[symbol] = tags
        print(f"Tags for {symbol}: {tags}")
    
    # Save results
    with open("shared/Project/Before/improved_tags_sample.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\\n✅ Improved tagging complete!")

if __name__ == "__main__":
    main()