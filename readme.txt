================================================================================
LINK DEL REPOSITORIO DONDE ESTAN LOS CSV
================================================================================

https://github.com/Hector-2710/proyecto_final.git


================================================================================
DOCUMENTACIÓN TÉCNICA: SISTEMA DE ANÁLISIS DE CINE (NEO4J + MONGODB)
================================================================================

1. DESCRIPCIÓN GENERAL
--------------------------------------------------------------------------------
Este script implementa una solución de "Persistencia Políglota". Combina una 
base de datos de grafos (Neo4j) para manejar relaciones complejas entre 
películas, actores y premios, con una base de datos documental (MongoDB) que 
almacena metadatos financieros y detalles de las películas.

El flujo principal consta de dos etapas:
1. ETL (Extracción, Transformación y Carga): Lee un CSV y puebla Neo4j.
2. ANÁLISIS: Ejecuta consultas cruzadas entre Neo4j (relaciones) y MongoDB 
   (finanzas/fechas).

2. REQUISITOS DEL SISTEMA
--------------------------------------------------------------------------------
A. Software Base:
   - Python 3.8 o superior.
   - MongoDB Server (corriendo en localhost:27017).
   - Neo4j Database (corriendo en localhost:7687).

B. Librerías de Python:
   Instalar las dependencias ejecutando el siguiente comando en la terminal:
   >> pip install pandas pymongo neo4j

C. Archivos de Datos:
   - 'full_data.csv': Debe estar en la misma carpeta que el script. Contiene 
     los datos históricos de los premios Oscar.
   - MongoDB ('movies'): La colección debe estar previamente poblada con datos 
     de películas (revenue, budget, dates, genres).

3. CONFIGURACIÓN Y CREDENCIALES
--------------------------------------------------------------------------------
Antes de ejecutar, revisa las siguientes variables al inicio del código:

- MONGO_URI:  Por defecto "mongodb://localhost:27017/"
- NEO4J_URI:  Por defecto "bolt://localhost:7687"
- NEO4J_AUTH: Actualmente configurado como ("neo4j", "Ingresa tu password de Neo4j").

4. INSTRUCCIONES DE EJECUCIÓN
--------------------------------------------------------------------------------
El script tiene un bloque principal "if __name__ == '__main__':" al final.

PASO A: PRIMERA EJECUCIÓN (CARGA DE DATOS)
Para cargar los datos del CSV a Neo4j por primera vez.
    limpiar_neo4j()      
    crear_constraints()  
    cargar_csv_a_neo4j(CSV_PATH)

Esto borrará la base de datos actual de Neo4j y cargará los datos limpios.
Este proceso puede tardar unos minutos dependiendo del tamaño del CSV.

PASO B: EJECUCIÓN DE ANÁLISIS
Una vez cargados los datos, puedes comentar las líneas de carga (para no repetir 
el proceso) y dejar descomentadas las funciones de análisis que desees ver:

    analizar_rentabilidad_best_picture()
    encontrar_blockbusters_ignorados()
    analizar_actores_rentables()
    consulta_generos_favoritos()
    analizar_estacionalidad_estrenos()
    analizar_duplas_exitosas()

PASO C: CORRER EL SCRIPT
Ejecuta en tu terminal:
   >> python nombre_de_tu_script.py

5. DESCRIPCIÓN DE FUNCIONES
--------------------------------------------------------------------------------

[ETL] limpiar_neo4j()
    Borra TODOS los nodos y relaciones en la base de datos Neo4j conectada. 
    Usar con precaución.

[ETL] cargar_csv_a_neo4j(csv_path)
    Lee el archivo 'full_data.csv'. Maneja problemas de encoding (UTF-8/Latin-1).
    Crea nodos: Pelicula, Persona, Categoria, Ceremonia.
    Crea relaciones: NOMINADA_EN, PARTICIPO_EN, GANO, etc.

[ANÁLISIS] analizar_rentabilidad_best_picture()
    1. Busca en Neo4j todas las películas nominadas a "BEST PICTURE".
    2. Cruza con MongoDB para obtener Presupuesto (budget) y Ganancias (revenue).
    3. Imprime cuáles fueron rentables y cuáles no.

[ANÁLISIS] encontrar_blockbusters_ignorados()
    Busca películas en MongoDB que generaron más de $100 Millones de ganancia 
    pero que NO existen en Neo4j (es decir, no fueron nominadas a premios).

[ANÁLISIS] analizar_actores_rentables()
    Identifica actores en Neo4j con 3 o más nominaciones principales.
    Calcula cuánto dinero generaron sus películas premiadas sumando datos de Mongo.

[ANÁLISIS] consulta_generos_favoritos()
    Filtra en MongoDB solo las películas que han sido nominadas (basado en Neo4j) 
    y cuenta cuáles son los géneros más frecuentes en los premios.

[ANÁLISIS] analizar_estacionalidad_estrenos()
    Analiza las fechas de estreno de las películas nominadas para determinar 
    en qué mes se suelen lanzar las películas ganadoras ("Oscar Season").

[ANÁLISIS] analizar_duplas_exitosas()
    Busca pares de personas que han trabajado juntas en al menos 3 películas 
    (Neo4j) y calcula el promedio de taquilla de esas colaboraciones (MongoDB).


================================================================================
FIN DE DOCUMENTACIÓN
================================================================================