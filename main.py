import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase

# CONFIGURACIÓN Y CONEXIONES
#  MongoDB 
MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
db_mongo = mongo_client["movies"]      
collection_movies = db_mongo["movies"]    

#  Neo4j (
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password") 
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

# FUNCIONES DE LIMPIEZA 
def limpiar_crew(crew_string):
    """
    Transforma el string sucio de MongoDB en una lista estructurada.
    """
    if not crew_string or not isinstance(crew_string, str): 
        return []
    
    parts = crew_string.split(', ')
    actors_list = []
    
    for i in range(0, len(parts), 2):
        if i+1 < len(parts):
            actors_list.append({
                "name": parts[i].strip(),
                "role": parts[i+1].strip()
            })
    return actors_list

def limpiar_neo4j():
    """Opcional: Borra Neo4j para evitar duplicados al re-correr el script"""
    print("🧹 Limpiando grafo antiguo en Neo4j...")
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("✅ Neo4j limpio.")

# SINCRONIZACIÓN (MONGO -> NEO4J)
def sincronizar_bases_de_datos():
    CANTIDAD_LIMITE = 1000    
    print(f"\n🚀 Iniciando sincronización (Limitado a {CANTIDAD_LIMITE} películas)...")

    # EXTRAER: Pedimos los documentos a Mongo
    cursor_mongo = collection_movies.find({}).limit(CANTIDAD_LIMITE)
    
    contador = 0
    with neo4j_driver.session() as session:
        for doc in cursor_mongo:
            # TRANSFORMAR: Preparamos los datos para el grafo
            # El _id de Mongo es un objeto ObjectId, lo pasamos a string
            movie_id_str = str(doc["_id"]) 
            title = doc.get("names", "Untitled")
            
            # Limpiamos el string de crew y genre que viene de Mongo
            actors_data = limpiar_crew(doc.get("crew", ""))
            genres_raw = doc.get("genre", "")
            genres_list = genres_raw.split(", ") if genres_raw else []
            
            # CARGAR: Escribimos en Neo4j
            cypher_query = """
            MERGE (m:Movie {id: $mid})
            SET m.title = $title
            
            // Relaciones de Género
            WITH m
            UNWIND $genres as g_name
            MERGE (g:Genre {name: trim(g_name)})
            MERGE (m)-[:BELONGS_TO]->(g)
            
            // Relaciones de Actores
            WITH m
            UNWIND $actors as actor
            MERGE (p:Person {name: actor.name})
            MERGE (p)-[:ACTED_IN {role: actor.role}]->(m)
            """
            
            session.run(cypher_query, 
                        mid=movie_id_str, 
                        title=title, 
                        genres=genres_list, 
                        actors=actors_data)
            
            contador += 1
            
    print(f"✅ Sincronización completada. {contador} películas procesadas.")

#  MÓDULO DE INTELIGENCIA DE NEGOCIO (3 Consultas)
def ejecutar_analisis_avanzado():
    print("\n" + "="*80)
    print("🧠 MÓDULO DE INTELIGENCIA DE NEGOCIOS (RESULTADOS CRUZADOS)")
    print("="*80)

    # ---------------------------------------------------------
    # CONSULTA 1: "Top Películas de Acción con Score Alto (>75)"
    # Objetivo: Recomendación de calidad.
    # ---------------------------------------------------------
    print("\n1️⃣  RECOMENDACIÓN: Mejores películas del género 'Action'")
    
    # Paso A: Neo4j encuentra los candidatos (Filtro Temático)
    target_genre = "Action"
    q1_neo = """
    MATCH (m:Movie)-[:BELONGS_TO]->(g:Genre {name: $genero})
    RETURN m.id as id
    """
    
    with neo4j_driver.session() as session:
        result = session.run(q1_neo, genero=target_genre)
        candidate_ids = [record["id"] for record in result]
    
    # Paso B: MongoDB filtra por calidad (Filtro Cualitativo)
    # Convertimos a ObjectId si es necesario, o string según tu base
    from bson.objectid import ObjectId
    try:
        obj_ids = [ObjectId(i) for i in candidate_ids]
    except:
        obj_ids = candidate_ids

    pipeline_q1 = [
        {"$match": {
            "_id": {"$in": obj_ids},
            "score": {"$gte": 75} # Solo buenas películas
        }},
        {"$project": {"names": 1, "score": 1, "_id": 0}},
        {"$sort": {"score": -1}},
        {"$limit": 5}
    ]
    
    top_movies = list(collection_movies.aggregate(pipeline_q1))
    
    print(f"   -> Encontramos {len(candidate_ids)} películas de '{target_genre}' en el Grafo.")
    print("   -> Top 5 con mejor Rating en MongoDB:")
    for m in top_movies:
        print(f"      ★ {m['score']} - {m['names']}")

    # CONSULTA 2: "Actores de Alto Presupuesto (Blockbuster Actors)"
    # Objetivo: Identificar talento que maneja grandes presupuestos.
    print("\n2️⃣  ACTORES BLOCKBUSTER: Participan en películas de > $100M")
    
    # Paso A: MongoDB encuentra el dinero (Filtro Financiero)
    min_budget = 100000000
    high_budget_movies = list(collection_movies.find(
        {"budget_x": {"$gt": min_budget}},
        {"_id": 1}
    ))
    
    # Extraemos los IDs como strings para Neo4j
    high_budget_ids = [str(doc["_id"]) for doc in high_budget_movies]
    
    # Paso B: Neo4j encuentra a las personas (Análisis de Red)
    q2_neo = """
    MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
    WHERE m.id IN $ids
    RETURN p.name as actor, count(m) as total_blockbusters
    ORDER BY total_blockbusters DESC
    LIMIT 5
    """
    
    print(f"   -> Películas de alto presupuesto encontradas: {len(high_budget_ids)}")
    print("   -> Actores que más aparecen en ellas:")
    
    with neo4j_driver.session() as session:
        result = session.run(q2_neo, ids=high_budget_ids)
        for record in result:
            print(f"      🎬 {record['actor']} (aparece en {record['total_blockbusters']} blockbusters)")

    # ---------------------------------------------------------
    # CONSULTA 3 (INVENTADA): "Rentabilidad Promedio por Género"
    # Objetivo: Decisión estratégica de inversión.
    # ---------------------------------------------------------
    print("\n3️⃣  ESTRATEGIA (Inventada): ¿Qué género genera más dinero promedio?")
    
    # Paso A: Neo4j agrupa IDs por Género
    q3_neo = """
    MATCH (m:Movie)-[:BELONGS_TO]->(g:Genre)
    RETURN g.name as genero, collect(m.id) as movie_ids
    """
    
    genre_revenue_data = []
    
    with neo4j_driver.session() as session:
        result = session.run(q3_neo)
        
        for record in result:
            genre = record["genero"]
            ids = record["movie_ids"]
            
            # Paso B: Mongo calcula el promedio de Revenue para esos IDs
            try:
                # Conversión de IDs (Manejo de errores si son strings u ObjectIds)
                valid_ids = []
                for i in ids:
                    try: valid_ids.append(ObjectId(i))
                    except: valid_ids.append(i)

                pipeline_q3 = [
                    {"$match": {"_id": {"$in": valid_ids}, "revenue": {"$gt": 0}}}, # Ignoramos revenue 0
                    {"$group": {"_id": None, "avg_rev": {"$avg": "$revenue"}}}
                ]
                
                res = list(collection_movies.aggregate(pipeline_q3))
                if res:
                    avg_revenue = res[0]['avg_rev']
                    genre_revenue_data.append((genre, avg_revenue))
            except Exception as e:
                pass # Ignorar errores puntuales de formato

    # Ordenar y mostrar top 5
    genre_revenue_data.sort(key=lambda x: x[1], reverse=True)
    
    print("   -> Ranking de Géneros por Ingreso Promedio:")
    for i, (gen, rev) in enumerate(genre_revenue_data[:5], 1):
        print(f"      {i}. {gen:<15} : ${rev:,.0f} (Promedio por película)")

# EJECUCIÓN
if __name__ == "__main__":
    try:
        limpiar_neo4j()             
        sincronizar_bases_de_datos() 
        ejecutar_analisis_avanzado() 
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        neo4j_driver.close()
        mongo_client.close()