#!/usr/bin/env python3
"""
Script pour compiler tous les domain_tags des entreprises dans un fichier JSON
sans doublons depuis le dossier shared/Project/advanced_tags/sector_classified_enhanced
"""

import json
import os
from pathlib import Path
from collections import Counter

def compile_domain_tags():
    # Chemin vers le dossier contenant les fichiers JSON
    source_dir = Path("/home/sagemaker-user/shared/Project/advanced_tags/sector_classified_enhanced")
    
    # Vérifier que le dossier existe
    if not source_dir.exists():
        print(f"Erreur: Le dossier {source_dir} n'existe pas")
        return
    
    # Ensemble pour stocker tous les tags uniques
    all_tags = set()
    # Compteur pour compter les occurrences de chaque tag
    tag_counter = Counter()
    
    # Parcourir tous les fichiers JSON dans le dossier
    json_files = list(source_dir.glob("*.json"))
    print(f"Traitement de {len(json_files)} fichiers JSON...")
    
    for json_file in json_files:
        # Ignorer les fichiers non-JSON
        if json_file.suffix != '.json':
            continue
            
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Extraire les domain_tags s'ils existent
            if 'domain_tags' in data and isinstance(data['domain_tags'], list):
                for tag in data['domain_tags']:
                    all_tags.add(tag)
                    tag_counter[tag] += 1
                    
        except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
            print(f"Erreur lors du traitement de {json_file.name}: {e}")
            continue
    
    # Créer le résultat final
    result = {
        "total_unique_tags": len(all_tags),
        "domain_tags": sorted(list(all_tags)),
        "tag_statistics": {
            "most_common_tags": tag_counter.most_common(10),
            "total_occurrences": sum(tag_counter.values()),
            "files_processed": len([f for f in json_files if f.suffix == '.json'])
        }
    }
    
    # Sauvegarder le résultat
    output_file = Path("/home/sagemaker-user/compiled_domain_tags.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Compilation terminée!")
    print(f"📊 {result['total_unique_tags']} tags uniques trouvés")
    print(f"📁 {result['tag_statistics']['files_processed']} fichiers traités")
    print(f"💾 Résultat sauvegardé dans: {output_file}")
    
    # Afficher les tags les plus fréquents
    print("\n🏆 Top 10 des tags les plus fréquents:")
    for tag, count in result['tag_statistics']['most_common_tags']:
        print(f"  - {tag}: {count} occurrences")

if __name__ == "__main__":
    compile_domain_tags()