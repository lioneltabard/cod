import duckdb
import pandas as pd
import os
from pathlib import Path
import datetime

def escape_pipe(value):
    """
    Remplace les caractères pipe (|) par le caractère (¦) pour éviter les problèmes de formatage Markdown
    
    Args:
        value: Valeur à échapper
        
    Returns:
        str: Valeur échappée
    """
    if value is None:
        return "NULL"
    return str(value).replace("|", "¦")

def create_schema_markdown(db_path, output_file="schema_duckdb.md", sensitive_fields=None, sensitive_tables=None):
    """
    Analyse la structure d'une base de données DuckDB et génère un rapport au format Markdown.
    
    Args:
        db_path (str): Chemin vers le fichier DuckDB
        output_file (str): Chemin du fichier markdown de sortie
        sensitive_fields (list, optional): Liste des noms de champs dont le contenu doit être masqué
        sensitive_tables (list, optional): Liste des noms de tables dont les données ne doivent pas être affichées
    """
    # Si aucune liste de champs/tables sensibles n'est fournie, initialiser une liste vide
    if sensitive_fields is None:
        sensitive_fields = []
    if sensitive_tables is None:
        sensitive_tables = []
    
    # Convertir les noms en minuscules pour une comparaison insensible à la casse
    sensitive_fields = [field.lower() for field in sensitive_fields]
    sensitive_tables = [table.lower() for table in sensitive_tables]
    
    # Établir la connexion à la base de données
    try:
        conn = duckdb.connect(db_path, read_only=True)
        print(f"Connexion établie à la base de données: {db_path}")
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données: {e}")
        return
    
    try:
        # Récupérer la liste des tables
        tables_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name;
        """
        
        tables = conn.execute(tables_query).fetchall()
        tables = [table[0] for table in tables]
        print(f"Nombre de tables trouvées: {len(tables)}")
        
        # Préparer le contenu markdown
        markdown_content = f"# Documentation du Schéma DuckDB\n\n"
        markdown_content += f"**Base de données:** {escape_pipe(db_path)}\n"
        markdown_content += f"**Date de génération:** {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        
        markdown_content += "## Résumé des Tables\n\n"
        markdown_content += "| Table | Colonnes | Lignes |\n"
        markdown_content += "|-------|----------|-------|\n"
        
        table_details = []
        
        # Pour chaque table, récupérer les informations
        for table in tables:
            try:
                # Récupérer les informations des colonnes
                columns_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = '{table}' AND table_schema = 'main'
                ORDER BY ordinal_position;
                """
                
                columns = conn.execute(columns_query).fetchall()
                
                # Compter le nombre d'enregistrements dans la table
                try:
                    count_query = f"""
                    SELECT COUNT(*) 
                    FROM "{table}" 
                    LIMIT 1;
                    """
                    row_count = conn.execute(count_query).fetchone()[0]
                except Exception as e:
                    print(f"Erreur lors du comptage des lignes de la table {table}: {e}")
                    row_count = "Erreur"
                
                # Ajouter au résumé
                markdown_content += f"| {escape_pipe(table)} | {len(columns)} | {row_count} |\n"
                
                # Stocker les détails pour plus tard
                table_details.append({
                    "name": table,
                    "columns": columns,
                    "row_count": row_count
                })
                
            except Exception as e:
                print(f"Erreur lors de l'analyse de la table {table}: {e}")
                markdown_content += f"| {escape_pipe(table)} | ERREUR | ERREUR |\n"
        
        # Ajouter les détails de chaque table
        markdown_content += "\n## Détails des Tables\n\n"
        
        for table_info in table_details:
            table_name = table_info["name"]
            columns = table_info["columns"]
            row_count = table_info["row_count"]
            
            markdown_content += f"### Table: {escape_pipe(table_name)}\n\n"
            markdown_content += f"**Nombre d'enregistrements:** {row_count}\n\n"
            markdown_content += "| Colonne | Type de données | Nullable | Valeur par défaut |\n"
            markdown_content += "|---------|----------------|----------|-------------------|\n"
            
            for column in columns:
                column_name, data_type, is_nullable, column_default = column
                column_default = escape_pipe(column_default) if column_default else "NULL"
                markdown_content += f"| {escape_pipe(column_name)} | {escape_pipe(data_type)} | {escape_pipe(is_nullable)} | {column_default} |\n"
            
            markdown_content += "\n"
            
            # Ajouter un échantillon de données (facultatif et limité)
            try:
                # Vérifier si la table est dans la liste des tables sensibles
                if table_name.lower() in sensitive_tables:
                    markdown_content += "#### Exemple de données\n\n"
                    markdown_content += "*Données masquées - Table identifiée comme sensible*\n\n"
                else:
                    sample_query = f"""
                    SELECT *
                    FROM "{table_name}"
                    LIMIT 5;
                    """
                    sample = conn.execute(sample_query).fetchdf()
                    
                    if not sample.empty:
                        markdown_content += "#### Exemple de données\n\n"
                        
                        # Masquer les données sensibles et échapper les caractères pipe
                        for column in sample.columns:
                            if column.lower() in sensitive_fields:
                                sample[column] = '******'
                        
                        # Échapper tous les caractères pipe dans le DataFrame
                        sample_escaped = sample.applymap(lambda x: escape_pipe(x))
                        
                        # Utiliser pandas pour convertir en markdown
                        markdown_table = sample_escaped.to_markdown(index=False)
                        markdown_content += markdown_table + "\n\n"
            except Exception as e:
                print(f"Impossible de récupérer un échantillon pour {table_name}: {e}")
        
        # Écrire le fichier markdown
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print(f"Documentation markdown générée avec succès: {output_file}")
        
    except Exception as e:
        print(f"Une erreur est survenue lors de l'analyse du schéma: {e}")
    finally:
        # Fermer la connexion
        conn.close()

if __name__ == "__main__":
    # Paramètres de connexion à la base de données
    db_path = input("Chemin vers le fichier DuckDB: (G:/temp/com_drfie_dbt_h.duckdb par défaut) ") or ("G:/temp/com_drfie_dbt_h.duckdb")
    output_file = input("Chemin du fichier markdown de sortie (schema_duckdb.md par défaut): ") or "schema_duckdb.md"
    
    # Liste des champs sensibles dont le contenu doit être masqué
    sensitive_fields = ['nss', 'ddn']
    print(f"Les champs suivants seront masqués dans les exemples de données: {', '.join(sensitive_fields)}")
    
    # Liste des tables sensibles dont les données ne doivent pas être affichées
    sensitive_tables = ['tRHsSIL', 'FAITS_MASSALgpn']
    print(f"Les tables suivantes n'afficheront pas d'exemples de données: {', '.join(sensitive_tables)}")
    
    # Exécuter la fonction
    create_schema_markdown(db_path, output_file, sensitive_fields, sensitive_tables)
