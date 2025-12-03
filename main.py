import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase
import math

MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
db_mongo = mongo_client["movies"]      
collection_movies = db_mongo["movies"]    

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password") # <--- CAMBIA TU CONTRASEÑA AQUÍ
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

CSV_PATH = "full_data.csv" 


def limpiar_neo4j():
    print("🧹 Limpiando grafo antiguo en Neo4j...")
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("✅ Neo4j limpio.")

def crear_constraints():
    """
    Crea índices para que la carga sea rápida y no se dupliquen nodos.
    """
    print("🛡️ Creando índices y restricciones...")
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Pelicula) REQUIRE p.titulo IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Persona) REQUIRE a.nombre IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (c:Ceremonia) ON (c.anio)",
        "CREATE INDEX IF NOT EXISTS FOR (cat:Categoria) ON (cat.nombre)"
    ]
    with neo4j_driver.session() as session:
        for q in queries:
            session.run(q)
    print("✅ Índices creados.")

def cargar_csv_a_neo4j(csv_path):
    print(f"📂 Leyendo archivo CSV: {csv_path}...")
    
    try:
        # Leemos el CSV detectando separador automáticamente
        df = pd.read_csv(csv_path, sep=None, engine='python', on_bad_lines='skip', encoding='utf-8')
    except UnicodeDecodeError:
        print("⚠️ Error de encoding. Intentando con 'latin-1'...")
        df = pd.read_csv(csv_path, sep=None, engine='python', on_bad_lines='skip', encoding='latin-1')
    except Exception as e:
        print(f"❌ Error crítico leyendo el CSV: {e}")
        return

    df = df.fillna("")
    
    print("🚀 Iniciando carga con relación [:GANO]...")
    
    # --- CAMBIOS EN LA CONSULTA CYPHER ---
    query_cypher = """
    // 1. Crear Nodos Base
    MERGE (cer:Ceremonia {anio: toString($year)})
    SET cer.numero = $ceremony_num

    MERGE (cat:Categoria {nombre: $category})
    SET cat.clase = $class_name
    MERGE (cat)-[:PRESENTADA_EN]->(cer)

    // 2. Crear Película
    WITH cer, cat
    WHERE $film IS NOT NULL AND toString($film) <> ""
    MERGE (m:Pelicula {titulo: toString($film)})
    MERGE (m)-[:NOMINADA_EN]->(cat)

    // 3. Crear Persona y Relación de Participación
    WITH cer, cat, m
    WHERE $name IS NOT NULL AND toString($name) <> ""
    MERGE (p:Persona {nombre: toString($name)})
    MERGE (p)-[:PARTICIPO_EN {rol_detalle: $detail}]->(m)
    
    // 4. LÓGICA DE GANADOR (¡NUEVO!)
    // Usamos FOREACH para simular un 'IF' en Cypher
    // Si el parámetro $is_winner es True, se crea la relación
    FOREACH (_ IN CASE WHEN $is_winner = true THEN [1] ELSE [] END |
        MERGE (p)-[:GANO {anio: toString($year)}]->(cat)
    )
    """

    with neo4j_driver.session() as session:
        count = 0
        for index, row in df.iterrows():
            if not str(row.get('Film', '')).strip():
                continue

            # Detectar si ganó (Kaggle suele usar true/false o 1/0)
            winner_val = row.get('winner', row.get('Winner', False))
            # Convertimos a booleano de Python seguro
            is_winner_bool = str(winner_val).lower() in ['true', '1', 'yes']

            params = {
                "year": str(row.get('Year', '')),
                "ceremony_num": row.get('Ceremony', 0),
                "category": str(row.get('Category', '')).strip().upper(),
                "class_name": str(row.get('Class', 'General')), 
                "film": str(row.get('Film', '')).strip(),
                "name": str(row.get('Name', '')).strip(),
                "detail": str(row.get('Detail', '')),
                "is_winner": is_winner_bool  # <--- Pasamos el booleano aquí
            }
            
            try:
                session.run(query_cypher, params)
                count += 1
                if count % 1000 == 0:
                    print(f"   ... procesadas {count} filas.")
            except Exception as e:
                print(f"⚠️ Error en fila {index}: {e}")
                continue

    print(f"✅ Carga finalizada. Total: {count} registros. Relaciones de GANADOR creadas.")

if __name__ == "__main__":
    try:
        count_movies = collection_movies.count_documents({})
        print(f"🟢 Conexión MongoDB exitosa. Películas en colección: {count_movies}")
    except Exception as e:
        print(f"🔴 Error conectando a MongoDB: {e}")

    try:
        limpiar_neo4j()      
        crear_constraints()  
        cargar_csv_a_neo4j(CSV_PATH)
        
    except Exception as e:
        print(f"🔴 Error en Neo4j: {e}")
    finally:
        neo4j_driver.close()
        mongo_client.close()