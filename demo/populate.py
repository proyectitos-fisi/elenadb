import csv
import subprocess

db_name = "demo.db"

formatted_querys = []
with open('fisi_2020.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        es_tercio = "true" if row['es_tercio'] == "SI" else "false"

        try:
            credits = int(row['creditos'])
        except ValueError:
            credits = 0

        formatted_query = (f"mete {{ codigo: \"{row['codigo']}\", nombre: \"{row['nombre']}\", "
                           f"tipo_documento: \"{row['tipo_documento']}\", documento: \"{row['documento']}\", "
                           f"creditos: {credits}, correo: \"{row['correo']}\", es_tercio: {es_tercio} }} "
                           f"en estudiantes pe")
        formatted_querys.append(formatted_query)

def run_query(query):
    print(query)
    result = subprocess.run(["elena", db_name, query], stdout=subprocess.PIPE, text=True)
    print(result.stdout)

run_query("creame tabla estudiantes { id int @id, codigo char(8) @unique, nombre char(255), tipo_documento char(24), documento char(13), creditos int, correo char(255), es_tercio bool, } pe")

for query in formatted_querys:
    run_query(query)
