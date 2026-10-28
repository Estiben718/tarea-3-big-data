import time
import json
import random
from kafka import KafkaProducer

# Configurar el productor de Kafka conectándose al broker local
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Listas de referencia basadas en el dataset real de becas
paises = ["España", "Francia", "Rusia", "Chile", "Hungría", "China", "India", "Reino Unido", "Estados Unidos"]
niveles = ["Maestría", "Doctorado", "Pregrado", "Curso corto", "Posgrado"]
rangos_edad = ["21 - 25 AÑOS", "26 - 30 AÑOS", "31 - 35 AÑOS", "MÁS DE 36 AÑOS", "16 - 20 AÑOS"]
generos = ["Femenino", "Masculino"]

print("=== INICIANDO PRODUCTOR DE STREAMING DE BECAS ===")
print("Enviando eventos a Kafka en tiempo real... (Presiona Ctrl+C para detener)")

try:
    while True:
        # Generar un evento aleatorio de postulación de beca
        evento_beca = {
            "pais_de_destino": random.choice(paises),
            "nivel_academico": random.choice(niveles),
            "rango_de_edad": random.choice(rangos_edad),
            "genero": random.choice(generos),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Enviar el mensaje al topic 'becas-stream'
        producer.send('becas-stream', value=evento_beca)
        print(f"Postulación enviada: {evento_beca}")
        
        # Pausa de 2 segundos entre cada evento simulado
        time.sleep(2)

except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")
    producer.close()
