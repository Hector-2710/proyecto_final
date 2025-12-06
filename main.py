import pandas as pd
import re
from pymongo import MongoClient
from neo4j import GraphDatabase
from datetime import datetime

MONGO_URI = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_URI)
db_mongo = mongo_client["movies"]      
collection_movies = db_mongo["movies"]    

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "neo4j123") 
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

def encontrar_blockbusters_ignorados():
    with neo4j_driver.session() as s:
        nominadas = {str(r["titulo"]).strip().lower() for r in s.run("MATCH (m:Pelicula) RETURN m.titulo AS titulo")}

    to_float = lambda x: float(str(x).replace("$", "").replace(",", "")) if x else 0.0
    candidatas = []

    cursor = collection_movies.find({}, {"names": 1, "budget_x": 1, "revenue": 1})
    
    for doc in cursor:
        rev, bud = to_float(doc.get("revenue")), to_float(doc.get("budget_x"))
        profit = rev - bud
        
        if profit > 100_000_000 and str(doc.get("names")).strip().lower() not in nominadas:
            candidatas.append({"titulo": doc.get("names"), "profit": profit, "revenue": rev})

    print(f"{'PELÍCULA':<40} | {'PROFIT':<15} | {'REVENUE'}")
    print("-" * 75)
    for p in sorted(candidatas, key=lambda x: x['profit'], reverse=True)[:10]:
        print(f"{str(p['titulo'])[:38]:<40} | ${p['profit']:<14,.0f} | ${p['revenue']:,.0f}")

def analizar_actores_rentables():    
    query = """ MATCH (p:Persona)-[:PARTICIPO_EN]->(m:Pelicula)-[:NOMINADA_EN]->(c:Categoria)
    WHERE c.nombre IN ["ACTOR IN A LEADING ROLE", "ACTRESS IN A LEADING ROLE"]
    WITH p, collect(DISTINCT m.titulo) as pelis, count(DISTINCT m) as num_peliculas_premiadas
    WHERE num_peliculas_premiadas >= 3 
    RETURN p.nombre as actor, pelis
    """
    
    actor_data = []
    with neo4j_driver.session() as s:
        for r in s.run(query):
            pelis_clean = [str(x).strip().lower() for x in r["pelis"]]
            actor_data.append((str(r["actor"]), pelis_clean))

    mongo_revs = {}
    to_float = lambda x: float(str(x).replace("$", "").replace(",", "")) if x else 0.0
    cursor = collection_movies.find({}, {"names": 1, "revenue": 1})
    for doc in cursor:
        mongo_revs[str(doc.get("names", "")).strip().lower()] = to_float(doc.get("revenue"))

    ranking = []
    for actor, pelis in actor_data:
        total_rev = 0
        hits = 0
        for p in pelis:
            if p in mongo_revs:
                total_rev += mongo_revs[p]
                hits += 1
        
        if hits > 0:
            ranking.append((actor, total_rev, hits))

    print(f"{'ACTOR/ACTRIZ':<25} | {'PELÍCULAS CRUZADAS':<18} | {'REVENUE TOTAL GENERADO'}")
    print("-" * 75)
    for name, rev, count in sorted(ranking, key=lambda x: x[1], reverse=True)[:10]:
        print(f"{name[:24]:<25} | {count:<18} | ${rev:,.0f}")

def consulta_generos_favoritos():
    
    with neo4j_driver.session() as s:
        result = s.run("MATCH (m:Pelicula) RETURN m.titulo AS titulo")
        titulos_nominados = [str(r["titulo"]).strip().lower() for r in result]

    pipeline = [
        {
            "$match": {"revenue": {"$exists": True}} 
        },
        {"$project": {"names": 1, "genre": 1, "_id": 0}}
    ]
    
    cursor = collection_movies.aggregate(pipeline)
    
    conteo_generos = {}
    
    for doc in cursor:
        nombre_mongo = str(doc.get("names", "")).strip().lower()
        
        if nombre_mongo in titulos_nominados:
            generos_str = doc.get("genre", "")
            if generos_str:
                lista_generos = [g.strip() for g in generos_str.split(',')]
                for genero in lista_generos:
                    conteo_generos[genero] = conteo_generos.get(genero, 0) + 1

    print(f"{'GÉNERO':<20} | {'CANTIDAD DE PELÍCULAS NOMINADAS'}")
    print("-" * 60)
    
    top_generos = sorted(conteo_generos.items(), key=lambda x: x[1], reverse=True)[:10]
    
    for genero, cantidad in top_generos:
        print(f"{genero:<20} | {cantidad}")

    return top_generos

def normalizar_titulo(texto):
    if not texto: return ""
    return re.sub(r'[^a-z0-9]', '', str(texto).lower())

def analizar_estacionalidad_estrenos():
    print("\n📅 ANALIZANDO ESTACIONALIDAD (¿Cuándo estrenar para ganar?)...")
    
    query_neo = """
    MATCH (m:Pelicula)-[:NOMINADA_EN]->(:Categoria)
    RETURN DISTINCT m.titulo as titulo
    """
    
    peliculas_nominadas = set()
    with neo4j_driver.session() as session:
        result = session.run(query_neo)
        for record in result:
            peliculas_nominadas.add(normalizar_titulo(record["titulo"]))
    
    print(f"   -> Películas nominadas cargadas: {len(peliculas_nominadas)}")

    meses_conteo = {i: 0 for i in range(1, 13)}
    matches = 0
    
    cursor = collection_movies.find({}, {"names": 1, "date_x": 1})
    
    for doc in cursor:
        try:
            nombre = normalizar_titulo(doc.get("names", ""))
            fecha_str = doc.get("date_x", "").strip()
            
            if nombre in peliculas_nominadas and fecha_str:
                dt = datetime.strptime(fecha_str, "%m/%d/%Y")
                meses_conteo[dt.month] += 1
                matches += 1
        except:
            continue

    nombres_meses = [
        "", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    
    print(f"   -> Se logró determinar la fecha de estreno de {matches} películas nominadas.\n")
    
    print(f"{'MES DE ESTRENO':<15} | {'CANTIDAD NOMINACIONES':<22} | {'% DEL TOTAL'}")
    print("-" * 60)
    
    for mes_num in range(1, 13):
        cantidad = meses_conteo[mes_num]
        porcentaje = (cantidad / matches * 100) if matches > 0 else 0
        print(f"{nombres_meses[mes_num]:<15} | {cantidad:<22} | {porcentaje:5.1f}%")

    q4 = meses_conteo[10] + meses_conteo[11] + meses_conteo[12]
    print(f"\n💡 DATO CLAVE: El {q4/matches*100:.1f}% de las nominadas se estrenaron en el último trimestre (Oct-Dic).")

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
        # analizar_rentabilidad_best_picture()
        # encontrar_blockbusters_ignorados()
        #analizar_actores_rentables()
        consulta_generos_favoritos()
        analizar_estacionalidad_estrenos()
    except Exception as e:
        print(f"🔴 Error en Neo4j: {e}")
    finally:
        neo4j_driver.close()
        mongo_client.close()