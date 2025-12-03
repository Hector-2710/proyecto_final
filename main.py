import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase
import math
import re

MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
db_mongo = mongo_client["movies"]      
collection_movies = db_mongo["movies"]    

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password") 
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

CSV_PATH = "full_data.csv" 


def limpiar_neo4j():
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def crear_constraints():
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Pelicula) REQUIRE p.titulo IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Persona) REQUIRE a.nombre IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (c:Ceremonia) ON (c.anio)",
        "CREATE INDEX IF NOT EXISTS FOR (cat:Categoria) ON (cat.nombre)"
    ]
    with neo4j_driver.session() as session:
        for q in queries:
            session.run(q)

def cargar_csv_a_neo4j(csv_path):    
    try:
        df = pd.read_csv(csv_path, sep=None, engine='python', on_bad_lines='skip', encoding='utf-8')
    except UnicodeDecodeError:
        print("⚠️ Error de encoding. Intentando con 'latin-1'...")
        df = pd.read_csv(csv_path, sep=None, engine='python', on_bad_lines='skip', encoding='latin-1')
    except Exception as e:
        print(f"❌ Error crítico leyendo el CSV: {e}")
        return

    df = df.fillna("")    
    query_cypher = """
    MERGE (cer:Ceremonia {anio: toString($year)})
    SET cer.numero = $ceremony_num

    MERGE (cat:Categoria {nombre: $category})
    SET cat.clase = $class_name
    MERGE (cat)-[:PRESENTADA_EN]->(cer)

    WITH cer, cat
    WHERE $film IS NOT NULL AND toString($film) <> ""
    MERGE (m:Pelicula {titulo: toString($film)})
    MERGE (m)-[:NOMINADA_EN]->(cat)

    WITH cer, cat, m
    WHERE $name IS NOT NULL AND toString($name) <> ""
    MERGE (p:Persona {nombre: toString($name)})
    MERGE (p)-[:PARTICIPO_EN {rol_detalle: $detail}]->(m)
    
    FOREACH (_ IN CASE WHEN $is_winner = true THEN [1] ELSE [] END |
        MERGE (p)-[:GANO {anio: toString($year)}]->(cat)
    )
    """

    with neo4j_driver.session() as session:
        count = 0
        for index, row in df.iterrows():
            if not str(row.get('Film', '')).strip():
                continue
            winner_val = row.get('winner', row.get('Winner', False))
            is_winner_bool = str(winner_val).lower() in ['true', '1', 'yes']

            params = {
                "year": str(row.get('Year', '')),
                "ceremony_num": row.get('Ceremony', 0),
                "category": str(row.get('Category', '')).strip().upper(),
                "class_name": str(row.get('Class', 'General')), 
                "film": str(row.get('Film', '')).strip(),
                "name": str(row.get('Name', '')).strip(),
                "detail": str(row.get('Detail', '')),
                "is_winner": is_winner_bool  
            }
            
            try:
                session.run(query_cypher, params)
                count += 1
                if count % 1000 == 0:
                    print(f"   ... procesadas {count} filas.")
            except Exception as e:
                print(f"⚠️ Error en fila {index}: {e}")
                continue

def analizar_rentabilidad_best_picture():
    with neo4j_driver.session() as session:
        query = """
        MATCH (m:Pelicula)-[:NOMINADA_EN]->(cat:Categoria)
        WHERE cat.nombre = "BEST PICTURE"
        RETURN m.titulo AS titulo
        """
        result = session.run(query)
        titulos = [r["titulo"] for r in result]

    reporte = []

    for title in titulos:
        movie = collection_movies.find_one({"names": title})

        if not movie:
            continue

        budget = movie.get("budget_x", 0) or 0
        revenue = movie.get("revenue", 0) or 0
        profit = revenue - budget
        rentable = profit > 0

        reporte.append((title, budget, revenue, profit, rentable))

    print("\n📊 RESULTADO DE RENTABILIDAD:")
    for title, budget, revenue, profit, rentable in reporte:
        print(f"🎬 {title} | Budget: {budget:,} | Revenue: {revenue:,} | "
              f"Profit: {profit:,} | Rentable: {rentable}")

if __name__ == "__main__":
    try:
        count_movies = collection_movies.count_documents({})
        print(f"🟢 Conexión MongoDB exitosa")
    except Exception as e:
        print(f"🔴 Error conectando a MongoDB: {e}")

    try:
        # limpiar_neo4j()      
        # crear_constraints()  
        # cargar_csv_a_neo4j(CSV_PATH)
        analizar_rentabilidad_best_picture()
        
    except Exception as e:
        print(f"🔴 Error en Neo4j: {e}")
    finally:
        neo4j_driver.close()
        mongo_client.close()