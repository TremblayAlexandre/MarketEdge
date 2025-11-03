import json
import os

# Secteurs autorisés
ALLOWED_SECTORS = [
    "Energy", "Materials", "Industrials", "Consumer Discretionary", 
    "Consumer Staples", "Health Care", "Financials", "Information Technology", 
    "Communication Services", "Utilities", "Real Estate"
]

# Mappings renforcés avec secteurs primaires et secondaires
ENHANCED_SECTOR_MAPPING = {
    # Tech avec Real Estate
    "ABNB": ("Information Technology", "Real Estate"),
    "Z": ("Information Technology", "Real Estate"),
    "ZG": ("Information Technology", "Real Estate"),
    
    # Fintech
    "V": ("Financials", "Information Technology"),
    "MA": ("Financials", "Information Technology"),
    "PYPL": ("Financials", "Information Technology"),
    "SQ": ("Financials", "Information Technology"),
    "COIN": ("Financials", "Information Technology"),
    
    # E-commerce avec retail
    "AMZN": ("Information Technology", "Consumer Discretionary"),
    "EBAY": ("Information Technology", "Consumer Discretionary"),
    "ETSY": ("Information Technology", "Consumer Discretionary"),
    
    # Media/Entertainment tech
    "NFLX": ("Communication Services", "Information Technology"),
    "DIS": ("Communication Services", "Consumer Discretionary"),
    "ROKU": ("Communication Services", "Information Technology"),
    
    # Healthcare tech
    "VEEV": ("Health Care", "Information Technology"),
    "TDOC": ("Health Care", "Information Technology"),
    
    # Energy tech
    "TSLA": ("Consumer Discretionary", "Information Technology"),
    
    # Industrial tech
    "UBER": ("Information Technology", "Consumer Discretionary"),
    "LYFT": ("Information Technology", "Consumer Discretionary"),
    
    # Real Estate tech
    "CSGP": ("Real Estate", "Information Technology"),
    
    # Traditional sectors
    "AAPL": ("Information Technology", None),
    "MSFT": ("Information Technology", None),
    "GOOGL": ("Communication Services", "Information Technology"),
    "GOOG": ("Communication Services", "Information Technology"),
    "META": ("Communication Services", "Information Technology"),
    "NVDA": ("Information Technology", None),
    "BRK.B": ("Financials", None),
    "JPM": ("Financials", None),
    "JNJ": ("Health Care", None),
    "PG": ("Consumer Staples", None),
    "XOM": ("Energy", None),
    "CVX": ("Energy", None),
    "WMT": ("Consumer Staples", None),
    "KO": ("Consumer Staples", None),
    "PEP": ("Consumer Staples", None),
    "ABBV": ("Health Care", None),
    "PFE": ("Health Care", None),
    "MRK": ("Health Care", None),
    "LLY": ("Health Care", None),
    "TMO": ("Health Care", None),
    "UNH": ("Health Care", None),
    "HD": ("Consumer Discretionary", None),
    "BAC": ("Financials", None),
    "WFC": ("Financials", None),
    "C": ("Financials", None),
    "GS": ("Financials", None),
    "MS": ("Financials", None),
    "AXP": ("Financials", None),
    "BLK": ("Financials", None),
    "SPG": ("Real Estate", None),
    "PLD": ("Real Estate", None),
    "AMT": ("Real Estate", None),
    "CCI": ("Real Estate", None),
    "EQIX": ("Real Estate", None),
    "NEE": ("Utilities", None),
    "SO": ("Utilities", None),
    "DUK": ("Utilities", None),
    "D": ("Utilities", None),
    "EXC": ("Utilities", None),
    "CAT": ("Industrials", None),
    "BA": ("Industrials", None),
    "GE": ("Industrials", None),
    "MMM": ("Industrials", None),
    "HON": ("Industrials", None),
    "UPS": ("Industrials", None),
    "FDX": ("Industrials", None),
    "LMT": ("Industrials", None),
    "RTX": ("Industrials", None),
    "NOC": ("Industrials", None),
    "DD": ("Materials", None),
    "DOW": ("Materials", None),
    "LIN": ("Materials", None),
    "APD": ("Materials", None),
    "ECL": ("Materials", None),
    "FCX": ("Materials", None),
    "NEM": ("Materials", None),
    "GOLD": ("Materials", None)
}

def classify_sector(ticker, domain_tags, data):
    """Classification renforcée des secteurs"""
    
    # Vérifier le mapping direct
    if ticker in ENHANCED_SECTOR_MAPPING:
        return ENHANCED_SECTOR_MAPPING[ticker]
    
    # Classification basée sur les domain_tags
    if domain_tags:
        tags_str = " ".join(domain_tags).lower()
        
        # Tech + Real Estate
        if any(term in tags_str for term in ["real estate", "property", "rental", "housing", "accommodation"]) and \
           any(term in tags_str for term in ["technology", "platform", "digital", "app", "software"]):
            return ("Information Technology", "Real Estate")
        
        # Fintech
        if any(term in tags_str for term in ["payment", "financial services", "banking", "credit"]) and \
           any(term in tags_str for term in ["technology", "digital", "platform"]):
            return ("Financials", "Information Technology")
        
        # Healthcare tech
        if any(term in tags_str for term in ["healthcare", "medical", "pharmaceutical", "biotech"]) and \
           any(term in tags_str for term in ["technology", "software", "digital"]):
            return ("Health Care", "Information Technology")
        
        # E-commerce
        if any(term in tags_str for term in ["e-commerce", "retail", "marketplace", "shopping"]) and \
           any(term in tags_str for term in ["technology", "platform", "digital"]):
            return ("Information Technology", "Consumer Discretionary")
        
        # Secteurs primaires
        if any(term in tags_str for term in ["software", "technology", "digital", "internet", "cloud", "ai", "data"]):
            return ("Information Technology", None)
        elif any(term in tags_str for term in ["pharmaceutical", "biotech", "medical", "healthcare"]):
            return ("Health Care", None)
        elif any(term in tags_str for term in ["banking", "financial", "insurance", "investment"]):
            return ("Financials", None)
        elif any(term in tags_str for term in ["energy", "oil", "gas", "renewable"]):
            return ("Energy", None)
        elif any(term in tags_str for term in ["real estate", "property", "reit"]):
            return ("Real Estate", None)
        elif any(term in tags_str for term in ["utilities", "electric", "water", "gas utility"]):
            return ("Utilities", None)
        elif any(term in tags_str for term in ["industrial", "manufacturing", "aerospace", "defense"]):
            return ("Industrials", None)
        elif any(term in tags_str for term in ["materials", "chemicals", "mining", "metals"]):
            return ("Materials", None)
        elif any(term in tags_str for term in ["media", "telecommunications", "entertainment"]):
            return ("Communication Services", None)
        elif any(term in tags_str for term in ["retail", "consumer goods", "automotive", "luxury"]):
            return ("Consumer Discretionary", None)
        elif any(term in tags_str for term in ["food", "beverage", "household", "staples"]):
            return ("Consumer Staples", None)
    
    # Défaut
    return ("Information Technology", None)

def process_companies():
    """Traiter toutes les compagnies et créer des fichiers individuels"""
    
    input_dir = "/home/sagemaker-user/shared/Project/enchanced_tags/combined_json"
    output_dir = "/home/sagemaker-user/shared/Project/enchanced_tags/sector_classified_cleaned"
    
    # Créer le dossier de sortie
    os.makedirs(output_dir, exist_ok=True)
    
    processed = 0
    
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            ticker = filename.replace('.json', '')
            
            try:
                # Lire le fichier
                with open(os.path.join(input_dir, filename), 'r', encoding='utf-8') as f:
                    company_data = json.load(f)
                
                # Extraire les données
                domain_tags = company_data.get('domain_tags', [])
                detailed_risk_analysis = company_data.get('detailed_risk_analysis', {})
                data = company_data.get('data', {})
                
                # Classifier les secteurs
                sector_primary, sector_secondary = classify_sector(ticker, domain_tags, data)
                
                # Créer la structure finale
                result = {
                    "ticker": ticker,
                    "sector_primary": sector_primary,
                    "sector_secondary": sector_secondary,
                    "domain_tags": domain_tags,
                    "detailed_risk_analysis": detailed_risk_analysis,
                    "data": data
                }
                
                # Sauvegarder le fichier individuel
                output_file = os.path.join(output_dir, f"{ticker}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                processed += 1
                
            except Exception as e:
                print(f"Erreur avec {ticker}: {e}")
    
    print(f"Traitement terminé: {processed} compagnies traitées")
    return processed

if __name__ == "__main__":
    process_companies()
    print(f"Traitement terminé: {processed} compagnies traitées")
    print(f"Fichiers sauvegardés dans: {output_dir}")

if __name__ == "__main__":
    process_companies()