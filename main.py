from parsing import parse_data, list_files_recursively
import pandas as pd
import json 
import os

def create_data(path_annotation, path_curation):


    # Lister les fichiers dans le dossier d'annotation
    files_annotation = list_files_recursively(path_annotation)
    # Garder les fichiers annotés par l'autre équipe
    annotators = [
        "AV00440", # Ben
        "AV45040", # JD
        "AU90360" # Max
    ]

    files_annotation = [file for file in files_annotation if any(annotator in file for annotator in annotators)]
    assert len(files_annotation) == 30, "Number of files in annotation folder is not 30"
    # Lister les fichiers dans le dossier de curation
    files_curation = list_files_recursively(path_curation)
    assert len(files_curation) == 10, "Number of files in curation folder is not 10"

    # Concatenate all DataFrames 
    dfs = []
    for file in files_annotation:
        df = parse_data(file)
        dfs.append(df)
    for file in files_curation:
        df = parse_data(file)
        dfs.append(df)
    
    final_df = pd.concat(dfs, ignore_index=True)
    # print(final_df.head())
    # print(final_df.shape)
    final_df.to_csv("data.csv", index=False)

    # Explode the "Texte" column
    exploded_df = final_df.copy()
    exploded_df['Texte'] = exploded_df['Texte'].str.split()
    exploded_df = exploded_df.explode('Texte')
    # print(exploded_df.head())
    # print(exploded_df.shape)
    exploded_df.to_csv("data_exploded.csv", index=False)


if __name__=="__main__":
    # === Create the data === #
    path_annotation = "data_json/annotation/"
    path_curation = "data_json/curation/"
    create_data(path_annotation, path_curation)

    # === Load the data === #
    data = pd.read_csv("data.csv")
    print(data.head())

    data_exploded = pd.read_csv("data_exploded.csv")
    print(data_exploded.head())
