import json
import re
from pathlib import Path

def extract_risk_factors_text(company_data):
    """Extract text from part1item1a (Risk Factors) section"""
    sections = company_data.get('data', {}).get('sections', {})
    part1a = sections.get('part1item1a', {})
    return part1a.get('text', '')

def parse_risk_factors(text):
    """Parse risk factors from text and categorize them"""
    if not text:
        return []
    
    # Common risk factor patterns
    risk_patterns = [
        r'(?:risks?|factors?|uncertainties|challenges|threats|concerns|issues|problems|difficulties|obstacles|vulnerabilities|exposures|liabilities|contingencies)',
        r'(?:may|could|might|would|will|can)\s+(?:adversely|negatively|materially|significantly|substantially)\s+(?:affect|impact|influence|harm|damage|hurt|impair|reduce|decrease|limit|restrict)',
        r'(?:competition|competitive|competitors|rivalry)',
        r'(?:regulation|regulatory|compliance|legal|litigation|lawsuit)',
        r'(?:economic|market|financial|credit|liquidity|capital)',
        r'(?:technology|technological|innovation|disruption|obsolescence)',
        r'(?:cybersecurity|security|data|privacy|breach)',
        r'(?:supply|supplier|vendor|third.party|partner)',
        r'(?:customer|client|demand|sales|revenue)',
        r'(?:operational|operations|business|strategic)',
        r'(?:environmental|climate|sustainability|ESG)',
        r'(?:geopolitical|political|trade|tariff|sanction)',
        r'(?:pandemic|epidemic|health|crisis|disaster)',
        r'(?:talent|personnel|employee|workforce|human)',
        r'(?:intellectual|property|patent|trademark|copyright)',
        r'(?:reputation|brand|image|public)',
        r'(?:acquisition|merger|integration|divestiture)',
        r'(?:debt|leverage|interest|financing|covenant)',
        r'(?:foreign|currency|exchange|international)',
        r'(?:tax|taxation|audit|examination)'
    ]
    
    # Extract sentences containing risk indicators
    sentences = re.split(r'[.!?]+', text)
    risk_sentences = []
    
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) > 50:  # Filter out very short sentences
            for pattern in risk_patterns:
                if re.search(pattern, sentence, re.IGNORECASE):
                    risk_sentences.append(sentence)
                    break
    
    return risk_sentences[:20]  # Limit to top 20 risk factors

def get_llm_risk_factors(ticker, existing_risks):
    """Generate additional risk factors using LLM knowledge"""
    
    # Industry-specific risk factors based on ticker knowledge
    industry_risks = {
        'technology': [
            'Rapid technological obsolescence and innovation cycles',
            'Cybersecurity threats and data breaches',
            'Intense competition from established and emerging players',
            'Regulatory changes in data privacy and AI governance',
            'Talent acquisition and retention challenges',
            'Platform dependency and ecosystem risks'
        ],
        'financial_services': [
            'Interest rate fluctuations and monetary policy changes',
            'Credit risk and loan defaults',
            'Regulatory compliance and capital requirements',
            'Market volatility and economic downturns',
            'Fintech disruption and digital transformation',
            'Fraud and operational risks'
        ],
        'healthcare': [
            'Regulatory approval delays and rejections',
            'Patent expirations and generic competition',
            'Clinical trial failures and safety concerns',
            'Healthcare policy and reimbursement changes',
            'Product liability and litigation risks',
            'Supply chain disruptions for critical materials'
        ],
        'energy': [
            'Commodity price volatility and market cycles',
            'Environmental regulations and climate policies',
            'Geopolitical tensions and supply disruptions',
            'Transition to renewable energy sources',
            'Operational safety and environmental incidents',
            'Capital intensive projects and financing risks'
        ],
        'retail': [
            'Consumer spending patterns and economic sensitivity',
            'E-commerce competition and digital transformation',
            'Supply chain disruptions and inventory management',
            'Seasonal demand fluctuations',
            'Labor costs and workforce management',
            'Brand reputation and customer loyalty risks'
        ],
        'manufacturing': [
            'Raw material cost inflation and availability',
            'Global supply chain vulnerabilities',
            'Trade tensions and tariff impacts',
            'Automation and workforce transition',
            'Quality control and product recalls',
            'Environmental compliance and sustainability'
        ]
    }
    
    # Company-specific risk factors
    company_risks = {
        'AAPL': ['iPhone dependency and product concentration', 'China market exposure and geopolitical risks'],
        'MSFT': ['Cloud competition and enterprise customer retention', 'AI regulation and ethical concerns'],
        'AMZN': ['AWS competition and margin pressure', 'Antitrust scrutiny and regulatory intervention'],
        'GOOGL': ['Advertising market cyclicality', 'Search algorithm changes and competition'],
        'TSLA': ['Production scaling and quality issues', 'Autonomous driving liability and regulation'],
        'META': ['User engagement and platform shifts', 'Content moderation and regulatory scrutiny'],
        'NVDA': ['GPU demand cyclicality', 'Geopolitical restrictions on chip exports'],
        'BRK.A': ['Insurance underwriting cycles', 'Investment portfolio concentration'],
        'JPM': ['Net interest margin compression', 'Credit cycle and loan loss provisions'],
        'JNJ': ['Pharmaceutical patent cliffs', 'Talc litigation and product liability']
    }
    
    # Determine primary industry
    primary_industry = 'technology'  # Default
    if any(word in ticker.lower() for word in ['bank', 'financial', 'capital', 'trust']):
        primary_industry = 'financial_services'
    elif ticker in ['PFE', 'JNJ', 'MRK', 'ABBV', 'LLY', 'BMY', 'AMGN', 'GILD']:
        primary_industry = 'healthcare'
    elif ticker in ['XOM', 'CVX', 'COP', 'EOG', 'SLB', 'HAL']:
        primary_industry = 'energy'
    elif ticker in ['WMT', 'HD', 'COST', 'TGT', 'LOW', 'TJX']:
        primary_industry = 'retail'
    elif ticker in ['CAT', 'MMM', 'GE', 'HON', 'UTX', 'BA']:
        primary_industry = 'manufacturing'
    
    # Get industry risks
    base_risks = industry_risks.get(primary_industry, industry_risks['technology'])
    
    # Add company-specific risks
    specific_risks = company_risks.get(ticker, [])
    
    # Combine and limit
    all_risks = base_risks + specific_risks
    
    # Filter out risks similar to existing ones
    filtered_risks = []
    for risk in all_risks:
        is_duplicate = False
        for existing in existing_risks:
            if any(word in existing.lower() for word in risk.lower().split()[:3]):
                is_duplicate = True
                break
        if not is_duplicate:
            filtered_risks.append(risk)
    
    return filtered_risks[:10]  # Limit to 10 additional risks

def analyze_risk_factors(ticker, company_data):
    """Analyze and summarize risk factors for a company"""
    
    # Extract risk factors text
    risk_text = extract_risk_factors_text(company_data)
    
    # Parse existing risk factors
    parsed_risks = parse_risk_factors(risk_text)
    
    # Get additional LLM-generated risks
    llm_risks = get_llm_risk_factors(ticker, parsed_risks)
    
    # Combine and categorize
    all_risks = []
    
    # Add parsed risks
    for risk in parsed_risks:
        all_risks.append({
            'type': 'parsed',
            'description': risk.strip(),
            'source': 'company_filing'
        })
    
    # Add LLM risks
    for risk in llm_risks:
        all_risks.append({
            'type': 'llm_generated',
            'description': risk,
            'source': 'industry_knowledge'
        })
    
    return {
        'total_risk_factors': len(all_risks),
        'parsed_from_filing': len(parsed_risks),
        'llm_generated': len(llm_risks),
        'risk_factors': all_risks
    }

def process_companies():
    """Process all companies and analyze their risk factors"""
    
    tagged_dir = Path('tagged_companies')
    output_dir = Path('risk_analysis')
    output_dir.mkdir(exist_ok=True)
    
    json_files = list(tagged_dir.glob('*.json'))
    print(f'Found {len(json_files)} companies to analyze')
    
    processed_count = 0
    
    # Process all companies
    for json_file in json_files:
        try:
            # Load company data
            with open(json_file, 'r', encoding='utf-8') as f:
                company_data = json.load(f)
            
            ticker = company_data.get('ticker', json_file.stem)
            print(f'Analyzing risk factors for {ticker}...')
            
            # Analyze risk factors
            risk_analysis = analyze_risk_factors(ticker, company_data)
            
            # Add risk analysis to company data
            company_data['risk_analysis'] = risk_analysis
            
            # Save updated file
            output_file = output_dir / f'{ticker}.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(company_data, f, indent=2, ensure_ascii=False)
            
            print(f'  Found {risk_analysis["total_risk_factors"]} risk factors ({risk_analysis["parsed_from_filing"]} parsed + {risk_analysis["llm_generated"]} generated)')
            processed_count += 1
            
        except Exception as e:
            print(f'Error processing {json_file.name}: {e}')
    
    print(f'\nCompleted! {processed_count} companies analyzed')

if __name__ == '__main__':
    process_companies()