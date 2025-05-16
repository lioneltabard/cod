import duckdb
import pandas as pd
import os
from pathlib import Path
import datetime

def create_schema_markdown(db_path, output_file="schema_duckdb.md"):
    """
    Analyse la structure d'une base de données DuckDB et génère un rapport au format Markdown.
    
    Args:
        db_path (str): Chemin vers le fichier DuckDB
        output_file (str): Chemin du fichier markdown de sortie
    """
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
        markdown_content += f"**Base de données:** {db_path}\n"
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
                markdown_content += f"| {table} | {len(columns)} | {row_count} |\n"
                
                # Stocker les détails pour plus tard
                table_details.append({
                    "name": table,
                    "columns": columns,
                    "row_count": row_count
                })
                
            except Exception as e:
                print(f"Erreur lors de l'analyse de la table {table}: {e}")
                markdown_content += f"| {table} | ERREUR | ERREUR |\n"
        
        # Ajouter les détails de chaque table
        markdown_content += "\n## Détails des Tables\n\n"
        
        for table_info in table_details:
            table_name = table_info["name"]
            columns = table_info["columns"]
            row_count = table_info["row_count"]
            
            markdown_content += f"### Table: {table_name}\n\n"
            markdown_content += f"**Nombre d'enregistrements:** {row_count}\n\n"
            markdown_content += "| Colonne | Type de données | Nullable | Valeur par défaut |\n"
            markdown_content += "|---------|----------------|----------|-------------------|\n"
            
            for column in columns:
                column_name, data_type, is_nullable, column_default = column
                column_default = column_default if column_default else "NULL"
                markdown_content += f"| {column_name} | {data_type} | {is_nullable} | {column_default} |\n"
            
            markdown_content += "\n"
            
            # Ajouter un échantillon de données (facultatif et limité)
            try:
                sample_query = f"""
                SELECT *
                FROM "{table_name}"
                LIMIT 5;
                """
                sample = conn.execute(sample_query).fetchdf()
                
                if not sample.empty:
                    markdown_content += "#### Exemple de données\n\n"
                    markdown_content += sample.to_markdown(index=False) + "\n\n"
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
    db_path = input("Chemin vers le fichier DuckDB: ")
    output_file = input("Chemin du fichier markdown de sortie (schema_duckdb.md par défaut): ") or "schema_duckdb.md"
    
    # Exécuter la fonction
    create_schema_markdown(db_path, output_file)
