import json 
import os
import sys
import pandas as pd


def parse_data(path_json):
    """
    Parse les données d'annotation depuis un fichier JSON d'annotation WebAnno.

    Args:
    path_json (str): Chemin relatif vers le fichier JSON d'annotation.

    Returns:
    pandas.DataFrame: Un DataFrame contenant les entités et relations extraites
    """
    # Charger le JSON
    with open(path_json, "r") as f:
        data = json.load(f)

    def extract_document_metadata(data):
        """Récupère le titre du document depuis les métadonnées."""
        doc_meta = next(
            fs for fs in data["%FEATURE_STRUCTURES"] 
            if fs.get("%TYPE") == "de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData"
        )
        return doc_meta.get("documentTitle", "")

    document_title = extract_document_metadata(data)

    def extract_annotator(data):
        """Récupère l'ID de l'annotateur."""
        doc_meta = next(
            fs for fs in data["%FEATURE_STRUCTURES"] 
            if fs.get("%TYPE") == "de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData"
        )
        return doc_meta.get("documentId", "")
    
    annotator_id = extract_annotator(data)

    # Étape 1: Récupérer l'ID du Sofa
    sofa_id = data["%VIEWS"]["_InitialView"]["%SOFA"]

    # Étape 2: Trouver l'entrée Sofa dans "%FEATURE_STRUCTURES"
    sofa_entry = next(
        fs for fs in data["%FEATURE_STRUCTURES"] 
        if fs["%ID"] == sofa_id and fs["%TYPE"] == "uima.cas.Sofa"
    )

    # Étape 3: Extraire le texte
    sofa_string = sofa_entry["sofaString"]

    # Dictionnaire pour mapper les %ID aux types médicaux
    string_arrays = {
        arr["%ID"]: arr["%ELEMENTS"][0] 
        for arr in data["%FEATURE_STRUCTURES"] 
        if arr.get("%TYPE") == "uima.cas.StringArray"
    }

    # Fonction pour extraire les entités par type
    def extract_entities(entity_type, fields, type_mapper=None):
        entities = []
        for entity in data["%FEATURE_STRUCTURES"]:
            if entity.get("%TYPE") == entity_type:
                entry = {"id": entity["%ID"]}
                for field in fields:
                    if field.startswith("@"):
                        entry[field] = entity.get(field)
                    else:
                        entry[field] = entity.get(field, "")
                if type_mapper and "@medical_type" in entry:
                    entry["type"] = type_mapper.get(entry["@medical_type"], "")
                entities.append(entry)
        return entities

    # Extraire les entités et relations
    medical_entities = extract_entities(
        "webanno.custom.Medical", 
        ["begin", "end", "CUI", "confidence", "@medical_type"], 
        string_arrays
    )

    abbreviation_entities = extract_entities(
        "webanno.custom.Abbreviation", 
        ["begin", "end", "abbreviation_type"]
    )

    causes_relations = extract_entities(
        "webanno.custom.Causes", 
        ["@Governor", "@Dependent"]
    )

    refers_to_relations = extract_entities(
        "webanno.custom.Refers_to", 
        ["@Governor", "@Dependent"]
    )
    # print("relations refer_to")
    # print(refers_to_relations)
    # print("relations causes")
    # print(causes_relations)

    # Lier les relations aux entités
    def resolve_relations(relations, entities, relation_type):
        resolved = []
        for rel in relations:
            governor = next((e for e in entities if e["id"] == rel["@Governor"]), None)
            dependent = next((e for e in entities if e["id"] == rel["@Dependent"]), None)
            if governor and dependent:
                resolved.append({
                    "Document": document_title,
                    "Annotateur": annotator_id,
                    "Relation": relation_type,
                    "Source": sofa_string[governor["begin"]:governor["end"]],
                    "Cible": sofa_string[dependent["begin"]:dependent["end"]]
                })
        return resolved

    # Générer les DataFrames
    df_medical = pd.DataFrame([{
        "Document": document_title,
        "Annotateur": annotator_id,
        "Texte": sofa_string[e["begin"]:e["end"]],
        "Début": e["begin"],
        "Fin": e["end"],
        "Layer": "Medical",
        "CUI": e["CUI"],
        "Type": e.get("type", ""),
        "Confiance": e.get("confidence", "")
    } for e in medical_entities])

    df_abbreviation = pd.DataFrame([{
        "Document": document_title,
        "Annotateur":annotator_id,
        "Texte": sofa_string[e["begin"]:e["end"]],
        "Début": e["begin"],
        "Fin": e["end"],
        "Layer": "Abbreviation",
        "Type": e.get("abbreviation_type", "")
    } for e in abbreviation_entities])

    df_causes = pd.DataFrame(resolve_relations(causes_relations, medical_entities, "Causes"))
    df_refers_to = pd.DataFrame(resolve_relations(refers_to_relations, abbreviation_entities, "Refers_to"))

    # print(f"Processed {document_title} by {annotator_id}")
    # print("Relation refers_to:" )
    # print(df_refers_to.head())
    # print("Relation causes:")   
    # print(df_causes.head())


    # Combiner tous les DataFrames
    final_df = pd.concat([df_medical, df_abbreviation, df_causes, df_refers_to], ignore_index=True)

    return final_df 

def list_files_recursively(directory):
    list_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            list_files.append(os.path.join(root, file))

    return list_files

if __name__=="__main__":
    path_json = "data_json/curation/common_28490813.txt/CURATION_USER.json"
    path_annotation = "data_json/annotation/"
    path_curation = "data_json/curation/"
    df = parse_data(path_json)
    print(df.head())
    df.to_csv("czcz.csv", index=False)

    # # Lister les fichiers dans le dossier d'annotation
    # files_annotation = list_files_recursively(path_annotation)
    # # DELETE FILES THAT STARTS WITH INITIAL_CAS (files that are not annotated)
    # files_annotation = [file for file in files_annotation if not file.endswith("INITIAL_CAS.json")] 
    # # KEEP FILES ANNOTED BY JD, Ben, and Max 
    # print("Files in annotation folder:\n")
    # print('-'*50)
    # print(files_annotation)
    # print('-'*50)


    # # Lister les fichiers dans le dossier de curation
    # files_curation = list_files_recursively(path_curation)
    # print("Files in curation folder:")
    # print(files_curation)

