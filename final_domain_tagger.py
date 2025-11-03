#!/usr/bin/env python3
"""
Script final pour redéfinir tous les domain tags des entreprises
avec une précision améliorée utilisant AWS Comprehend
"""

import json
import os
import boto3
import time
import re
from typing import List, Dict, Set, Tuple
from pathlib import Path
from collections import Counter

class FinalDomainTagger:
    def __init__(self, region='us-west-2'):
        """Initialize with comprehensive industry mappings"""
        self.comprehend = boto3.client('comprehend', region_name=region)
        self.processed_tags = set()
        
        # Expanded industry keyword mapping
        self.industry_keywords = {
            'biotechnology': ['biotech', 'biotechnology', 'biopharmaceutical', 'biologics', 'therapeutic', 'drug development', 'clinical trials', 'pharmaceutical research', 'molecular', 'protein', 'antibody', 'vaccine', 'gene therapy', 'immunology', 'oncology'],
            'pharmaceuticals': ['pharmaceutical', 'pharma', 'medicine', 'drug', 'medication', 'prescription', 'healthcare products', 'medical devices', 'diagnostic', 'treatment', 'therapy', 'clinical', 'fda approval', 'medical research'],
            'healthcare': ['healthcare', 'health care', 'medical', 'hospital', 'clinic', 'patient', 'physician', 'doctor', 'nurse', 'medical services', 'health insurance', 'medical equipment', 'healthcare provider'],
            'technology': ['software', 'technology', 'tech', 'digital', 'artificial intelligence', 'machine learning', 'data analytics', 'cybersecurity', 'blockchain', 'internet', 'mobile app', 'platform', 'innovation'],
            'cloud_services': ['cloud services', 'aws', 'azure', 'google cloud', 'saas', 'paas', 'iaas', 'web services', 'cloud computing', 'cloud infrastructure', 'cloud platform', 'data center'],
            'financial_services': ['banking', 'financial services', 'investment', 'insurance', 'credit', 'loan', 'mortgage', 'wealth management', 'asset management', 'trading', 'fintech', 'payment', 'financial'],
            'consumer_electronics': ['consumer electronics', 'smartphone', 'tablet', 'laptop', 'television', 'gaming', 'wearable', 'smart device', 'electronics manufacturing', 'mobile device'],
            'automotive': ['automotive', 'automobile', 'car', 'vehicle', 'truck', 'electric vehicle', 'autonomous driving', 'auto parts', 'transportation equipment', 'mobility'],
            'energy': ['energy', 'oil', 'gas', 'petroleum', 'renewable energy', 'solar', 'wind', 'nuclear', 'utilities', 'power generation', 'electricity', 'utility', 'electric'],
            'telecommunications': ['telecommunications', 'telecom', 'wireless', 'broadband', 'network', 'mobile network', '5g', 'fiber optic', 'satellite', 'communication'],
            'retail': ['retail', 'e-commerce', 'online shopping', 'store', 'merchandise', 'consumer goods', 'fashion', 'apparel', 'grocery', 'shopping'],
            'media_entertainment': ['media', 'entertainment', 'streaming', 'content', 'television', 'movie', 'music', 'gaming', 'publishing', 'broadcasting', 'digital content'],
            'manufacturing': ['manufacturing', 'production', 'factory', 'industrial', 'assembly', 'fabrication', 'processing', 'machinery', 'equipment', 'industrial equipment'],
            'aerospace_defense': ['aerospace', 'defense', 'aviation', 'aircraft', 'satellite', 'military', 'space', 'rocket', 'missile', 'defense contractor'],
            'real_estate': ['real estate', 'property', 'construction', 'building', 'development', 'commercial property', 'residential', 'reit'],
            'logistics': ['logistics', 'supply chain', 'shipping', 'transportation', 'delivery', 'warehouse', 'distribution', 'freight', 'transport'],
            'agriculture': ['agriculture', 'farming', 'food production', 'crop', 'livestock', 'agricultural equipment', 'fertilizer', 'seeds', 'agribusiness'],
            'semiconductor': ['semiconductor', 'chip', 'microprocessor', 'integrated circuit', 'silicon', 'wafer', 'electronics components', 'chipset'],
            'education': ['education', 'school', 'university', 'learning', 'training', 'educational services', 'e-learning', 'academic'],
            'mining': ['mining', 'metals', 'minerals', 'extraction', 'copper', 'gold', 'iron', 'coal', 'natural resources', 'commodity'],
            'chemicals': ['chemicals', 'chemical products', 'specialty chemicals', 'industrial chemicals', 'petrochemicals', 'chemical manufacturing'],
            'food_beverage': ['food', 'beverage', 'restaurant', 'food processing', 'dairy', 'snacks', 'drinks', 'nutrition', 'food service'],
            'textiles': ['textiles', 'clothing', 'apparel', 'fashion', 'fabric', 'garment', 'footwear', 'textile manufacturing'],
            'hospitality': ['hospitality', 'hotel', 'resort', 'travel', 'tourism', 'accommodation', 'leisure', 'vacation'],
            'consulting': ['consulting', 'advisory', 'professional services', 'business services', 'management consulting', 'consulting services'],
            'waste_management': ['waste management', 'recycling', 'environmental services', 'waste disposal', 'sanitation', 'environmental'],
            'mobile': ['mobile', 'smartphone', 'cellular', 'wireless device', 'mobile technology', 'mobile services'],
            'industrial': ['industrial', 'heavy industry', 'industrial equipment', 'industrial services', 'industrial manufacturing']
        }
        
        # Known company mappings for accuracy
        self.company_mappings = {
            'ABBV': ['biotechnology', 'pharmaceuticals'],
            'AMZN': ['retail', 'cloud_services', 'technology', 'logistics'],
            'AAPL': ['consumer_electronics', 'technology', 'mobile'],
            'MSFT': ['technology', 'cloud_services'],
            'GOOGL': ['technology', 'cloud_services', 'media_entertainment'],
            'GOOG': ['technology', 'cloud_services', 'media_entertainment'],
            'TSLA': ['automotive', 'energy'],
            'JNJ': ['pharmaceuticals', 'healthcare', 'consumer_electronics'],
            'PFE': ['pharmaceuticals', 'biotechnology'],
            'MRK': ['pharmaceuticals', 'biotechnology'],
            'XOM': ['energy'],
            'CVX': ['energy'],
            'JPM': ['financial_services'],
            'BAC': ['financial_services'],
            'WFC': ['financial_services'],
            'WMT': ['retail'],
            'V': ['financial_services'],
            'MA': ['financial_services'],
            'NVDA': ['semiconductor', 'technology'],
            'AMD': ['semiconductor', 'technology'],
            'INTC': ['semiconductor', 'technology'],
            'CRM': ['technology', 'cloud_services'],
            'ORCL': ['technology', 'cloud_services'],
            'IBM': ['technology', 'cloud_services', 'consulting'],
            'NFLX': ['media_entertainment', 'technology'],
            'DIS': ['media_entertainment'],
            'HD': ['retail'],
            'LOW': ['retail'],
            'TGT': ['retail'],
            'KO': ['food_beverage'],
            'PEP': ['food_beverage'],
            'MCD': ['food_beverage'],
            'NKE': ['textiles', 'retail'],
            'UNH': ['healthcare', 'financial_services'],
            'CVS': ['healthcare', 'retail'],
            'TMO': ['healthcare', 'biotechnology'],
            'DHR': ['healthcare', 'technology'],
            'ABT': ['healthcare', 'pharmaceuticals'],
            'BA': ['aerospace_defense'],
            'LMT': ['aerospace_defense'],
            'RTX': ['aerospace_defense'],
            'CAT': ['manufacturing', 'industrial'],
            'DE': ['manufacturing', 'agriculture'],
            'MMM': ['manufacturing', 'industrial'],
            'GE': ['manufacturing', 'energy', 'aerospace_defense'],
            'F': ['automotive'],
            'GM': ['automotive'],
            'VZ': ['telecommunications'],
            'T': ['telecommunications'],
            'TMUS': ['telecommunications']
        }
    
    def extract_business_context(self, text: str) -> str:
        """Extract business-relevant context from company filings"""
        # Remove legal boilerplate and focus on business description
        text = re.sub(r'Item \d+[A-Z]*\..*?Risk Factors', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'PART [IVX]+', '', text)
        
        # Extract business-focused sentences
        sentences = text.split('.')
        business_sentences = []
        
        business_indicators = [
            'company', 'business', 'products', 'services', 'operations', 'customers',
            'market', 'industry', 'segment', 'revenue', 'sales', 'manufacturing',
            'develops', 'provides', 'offers', 'sells', 'operates'
        ]
        
        for sentence in sentences[:100]:  # First 100 sentences
            sentence_lower = sentence.strip().lower()
            if len(sentence_lower) > 20 and any(indicator in sentence_lower for indicator in business_indicators):
                business_sentences.append(sentence.strip())
                if len(' '.join(business_sentences)) > 2500:
                    break
        
        return '. '.join(business_sentences)
    
    def analyze_with_comprehend(self, text: str) -> Dict[str, float]:
        """Enhanced Comprehend analysis with better scoring"""
        domain_scores = {}
        
        try:
            # Split text if too long
            if len(text.encode('utf-8')) > 4500:
                text = text[:4500]
            
            # Detect entities
            entities_response = self.comprehend.detect_entities(
                Text=text,
                LanguageCode='en'
            )
            
            # Detect key phrases
            phrases_response = self.comprehend.detect_key_phrases(
                Text=text,
                LanguageCode='en'
            )
            
            # Combine all text elements
            all_elements = []
            
            # Add entities with higher weight for ORGANIZATION types
            for entity in entities_response.get('Entities', []):
                entity_text = entity.get('Text', '').lower()
                entity_type = entity.get('Type', '')
                confidence = entity.get('Score', 0)
                
                weight = 2 if entity_type == 'ORGANIZATION' else 1
                if confidence > 0.8:
                    weight *= 2
                
                all_elements.append((entity_text, weight))
            
            # Add key phrases
            for phrase in phrases_response.get('KeyPhrases', []):
                phrase_text = phrase.get('Text', '').lower()
                confidence = phrase.get('Score', 0)
                
                weight = 2 if confidence > 0.8 else 1
                all_elements.append((phrase_text, weight))
            
            # Score domains based on keyword matches
            for domain, keywords in self.industry_keywords.items():
                score = 0
                
                for element_text, element_weight in all_elements:
                    for keyword in keywords:
                        if keyword.lower() in element_text:
                            # Multi-word keywords get higher scores
                            keyword_weight = len(keyword.split()) + 1
                            score += keyword_weight * element_weight
                
                if score > 0:
                    domain_scores[domain] = score
            
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print(f"  Comprehend error: {e}")
        
        return domain_scores
    
    def get_final_domains(self, domain_scores: Dict[str, float], ticker: str) -> List[str]:
        """Get final domain tags with improved logic"""
        # Use company-specific mapping if available
        if ticker in self.company_mappings:
            return self.company_mappings[ticker]
        
        if not domain_scores:
            return []
        
        # Sort by score
        sorted_domains = sorted(domain_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Dynamic threshold based on top score
        max_score = sorted_domains[0][1]
        min_threshold = max(3, max_score * 0.15)
        
        # Select domains above threshold
        selected_domains = []
        for domain, score in sorted_domains:
            if score >= min_threshold and len(selected_domains) < 6:
                selected_domains.append(domain)
        
        return selected_domains
    
    def process_company(self, company_file: str) -> Dict:
        """Process single company with enhanced accuracy"""
        try:
            with open(company_file, 'r', encoding='utf-8') as f:
                company_data = json.load(f)
            
            ticker = company_data.get('ticker', 'UNKNOWN')
            
            # Extract business context
            part1_text = company_data.get('data', {}).get('sections', {}).get('part1', {}).get('text', '')
            
            if not part1_text:
                if ticker in self.company_mappings:
                    company_data['domain_tags'] = self.company_mappings[ticker]
                return company_data
            
            business_context = self.extract_business_context(part1_text)
            
            if len(business_context) < 100:
                if ticker in self.company_mappings:
                    company_data['domain_tags'] = self.company_mappings[ticker]
                return company_data
            
            # Analyze with Comprehend
            domain_scores = self.analyze_with_comprehend(business_context)
            
            # Get final domains
            final_domains = self.get_final_domains(domain_scores, ticker)
            
            if final_domains:
                company_data['domain_tags'] = final_domains
                self.processed_tags.update(final_domains)
            
            return company_data
            
        except Exception as e:
            print(f"  Error processing {company_file}: {e}")
            return None
    
    def process_all_companies(self, input_dir: str, output_dir: str):
        """Process all companies with progress tracking"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        company_files = list(input_path.glob("*.json"))
        total_files = len(company_files)
        
        print(f"Processing {total_files} companies...")
        
        processed = 0
        failed = 0
        
        for i, company_file in enumerate(company_files, 1):
            try:
                print(f"[{i:3d}/{total_files}] {company_file.stem}...", end=" ")
                
                result = self.process_company(str(company_file))
                
                if result:
                    # Save result
                    output_file = output_path / company_file.name
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, ensure_ascii=False)
                    
                    tags = result.get('domain_tags', [])
                    print(f"✓ {len(tags)} tags: {tags}")
                    processed += 1
                else:
                    print("✗ Failed")
                    failed += 1
                
                # Rate limiting
                if i % 10 == 0:
                    time.sleep(2)
                else:
                    time.sleep(0.3)
                
            except Exception as e:
                print(f"✗ Error: {e}")
                failed += 1
        
        print(f"\n=== SUMMARY ===")
        print(f"Processed: {processed}")
        print(f"Failed: {failed}")
        print(f"Total unique tags: {len(self.processed_tags)}")
        print(f"Tags: {sorted(list(self.processed_tags))}")
        
        # Save final tags list
        tags_file = output_path.parent / "final_domain_tags.json"
        with open(tags_file, 'w') as f:
            json.dump(sorted(list(self.processed_tags)), f, indent=2)
        
        print(f"Saved tags to: {tags_file}")

def main():
    """Main execution"""
    input_dir = "/home/sagemaker-user/shared/Project/Before/tagged_companies"
    output_dir = "/home/sagemaker-user/shared/Project/Before/final_tagged_companies"
    
    tagger = FinalDomainTagger()
    tagger.process_all_companies(input_dir, output_dir)

if __name__ == "__main__":
    main()