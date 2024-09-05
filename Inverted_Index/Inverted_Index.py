import string
import threading
from collections import defaultdict
import os
import spacy
from nltk.stem import PorterStemmer
from concurrent.futures import ThreadPoolExecutor

# Cargar el modelo de spaCy
nlp = spacy.load("en_core_web_sm")
stemmer = PorterStemmer()
batch_size = 4
max_size_chunk = 1000000  # Tamaño máximo para spaCy

def procesar_chunk(chunk):
    if len(chunk) > max_size_chunk:
        chunk = chunk[:max_size_chunk]  # Truncar el texto si es demasiado largo
    
    indice_invertido_chunk = defaultdict(set)
    doc = nlp(chunk)
    for i, token in enumerate(doc):
        token_stemmed = stemmer.stem(token.text)
        indice_invertido_chunk[token_stemmed].add(i)
    return indice_invertido_chunk

def procesar_archivo(ruta_archivo):
    indice_invertido = defaultdict(set)
    try:
        print(f"Lectura del archivo: {ruta_archivo}")
        with open(ruta_archivo, "r", encoding="utf-8") as archivo:
            texto = archivo.read()
            size_chunk = len(texto) // batch_size + 1
            chunks = [texto[i:i+size_chunk] for i in range(0, len(texto), size_chunk)]
            print(f"Procesando {len(chunks)} chunks del archivo: {ruta_archivo}")
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                indices_invertidos = list(executor.map(procesar_chunk, chunks))
                for indice in indices_invertidos:
                    for termino, posiciones in indice.items():
                        indice_invertido[termino].add(os.path.basename(ruta_archivo))  # Guardar solo el nombre del archivo
    except Exception as e:
        print(f"Error al procesar el archivo {ruta_archivo}: {e}")
    return indice_invertido

indice_invertido_global = defaultdict(set)

def procesar_archivos_thread(rutas_archivos):
    for ruta_archivo in rutas_archivos:
        print(f"Procesando archivo: {ruta_archivo}")
        indice_invertido = procesar_archivo(ruta_archivo)
        for termino, archivos in indice_invertido.items():
            indice_invertido_global[termino].update(archivos)

directorio = "C:/Users/Usuario/Desktop/Big Data/"  # Actualizado a la nueva ruta
threads = []
rutas_archivos = [os.path.join(directorio, nombre_archivo) for nombre_archivo in os.listdir(directorio) if nombre_archivo.endswith(".txt")]

for i in range(0, len(rutas_archivos), batch_size):
    batch_archivos = rutas_archivos[i:i+batch_size]
    thread = threading.Thread(target=procesar_archivos_thread, args=(batch_archivos,))
    thread.start()
    threads.append(thread)  # Agregar hilo a la lista

for thread in threads:
    thread.join()

# Guardar el índice invertido en un archivo de salida
ruta_archivo_salida = "C:/Users/Usuario/Desktop/Big Data/indice_invertido_salida.txt"
with open(ruta_archivo_salida, "w", encoding="utf-8") as archivo_salida:
    for termino, archivos in indice_invertido_global.items():
        lista_archivos = ', '.join(archivos)
        archivo_salida.write(f"{termino}: {lista_archivos}\n")

print(f"Índice invertido guardado en {ruta_archivo_salida}")
