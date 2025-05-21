import tkinter as tk
from tkinter import ttk
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count
import pandas as pd
from PIL import Image, ImageTk

# Inicio de Python Spark
spark = SparkSession.builder.appName("COVID GUI").getOrCreate()
#cargar dataframe
df = spark.read.option("header", "true").option("inferSchema", "true").csv("Casos_positivos_de_COVID-19_en_Colombia._20240501.csv")
# Convertir columna de fecha de diagnóstico a tipo fecha
df = df.withColumn("Fecha_diagnostico", to_date(col("Fecha de diagnóstico"), "yyyy-MM-dd"))

#cargar los departamentos
departamentos = df.select("Nombre departamento").distinct().rdd.flatMap(lambda x: x).collect()
departamentos.sort()

# Cantidad de casos de contagio por edad
def grafico_por_edad():
    df_age = df.groupBy("Edad").count().orderBy("Edad")
    pd_age = df_age.toPandas()
    plt.figure(figsize=(10,6))
    plt.plot(pd_age['Edad'], pd_age['count'], marker='o', linestyle='-')
    plt.title("Casos por Edad")
    plt.xlabel("Edad")
    plt.ylabel("Cantidad")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Cantidad de casos de contagio por departamento
def grafico_por_departamento():
    df_dep = df.groupBy("Nombre departamento").count().orderBy("count", ascending=False).toPandas()
    plt.figure(figsize=(10,8))
    plt.barh(df_dep["Nombre departamento"], df_dep["count"], color='skyblue')
    plt.title("Casos por Departamento")
    plt.xlabel("Cantidad")
    plt.ylabel("Departamento")
    plt.tight_layout()
    plt.show()

# Cantidad de casos de contagio por sexo
def grafico_por_sexo():
    df_sex = df.groupBy("Sexo").count().toPandas()
    plt.figure(figsize=(6,5))
    plt.bar(df_sex["Sexo"], df_sex["count"], color='orange')
    plt.title("Casos por Sexo")
    plt.xlabel("Sexo")
    plt.ylabel("Cantidad")
    plt.tight_layout()
    plt.show()

# Porcentaje de recuperados
def porcentaje_recuperados():
    total = df.count()
    recuperados = df.filter(col("Recuperado") == "Recuperado").count()
    porcentaje = (recuperados / total) * 100
    labels = ['Recuperados', 'Otros']
    values = [porcentaje, 100 - porcentaje]

    plt.figure(figsize=(6,6))
    plt.pie(values, labels=labels, autopct='%1.1f%%', colors=['green', 'red'])
    plt.title("Porcentaje de Recuperados")
    plt.show()

# Evolución de casos por fecha de diagnóstico
def evolucion_por_fecha():
    df_fecha = df.groupBy("Fecha_diagnostico").agg(count("*").alias("cantidad")).orderBy("Fecha_diagnostico").toPandas()
    plt.figure(figsize=(12,6))
    plt.plot(df_fecha["Fecha_diagnostico"], df_fecha["cantidad"], color='purple')
    plt.title("Evolución de Casos por Fecha de Diagnóstico")
    plt.xlabel("Fecha")
    plt.ylabel("Número de Casos")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Porcentaje de enfermos por tipo de contagio
def grafico_por_tipo_contagio():
    df_contagio = df.groupBy("Tipo de contagio").count().toPandas()
    plt.figure(figsize=(7,7))
    plt.pie(df_contagio["count"], labels=df_contagio["Tipo de contagio"], autopct='%1.1f%%', startangle=140)
    plt.title("Porcentaje de Casos por Tipo de Contagio")
    plt.axis('equal')  
    plt.tight_layout()
    plt.show()

def abrir_ventana_departamento():
    # Crear ventana secundaria
    ventana_dep = tk.Toplevel()
    ventana_dep.title("Estadísticas por Departamento")
    ventana_dep.geometry("400x400")

    # Variable para selección
    seleccion = tk.StringVar()
    seleccion.set(departamentos[0])  # valor por defecto

    # Dropdown para seleccionar departamento
    tk.Label(ventana_dep, text="Selecciona un departamento:").pack(pady=10)
    menu = tk.OptionMenu(ventana_dep, seleccion, *departamentos)
    menu.pack()

    # Texto donde se mostrarán los resultados
    texto_resultado = tk.Text(ventana_dep, height=10, width=50)
    texto_resultado.pack(pady=10)

    # Función para mostrar estadísticas del departamento seleccionado
    def mostrar_estadisticas():
        depto = seleccion.get()
        df_filtro = df.filter(df["Nombre departamento"] == depto)

        total = df_filtro.count()
        hombres = df_filtro.filter(df["Sexo"] == "M").count()
        mujeres = df_filtro.filter(df["Sexo"] == "F").count()
        edad_prom = df_filtro.selectExpr("avg(Edad)").first()[0]

        resultado = (
            f"Departamento: {depto}\n"
            f"Total casos: {total}\n"
            f"Hombres: {hombres} ({(hombres/total)*100:.2f}%)\n"
            f"Mujeres: {mujeres} ({(mujeres/total)*100:.2f}%)\n"
            f"Edad promedio: {edad_prom:.2f} años\n"
        )

        texto_resultado.delete("1.0", tk.END)
        texto_resultado.insert(tk.END, resultado)

    # Botón para mostrar estadísticas
    tk.Button(ventana_dep, text="Mostrar estadísticas", command=mostrar_estadisticas, bg="#3F51B5", fg="white").pack(pady=5)

# crear la interfaz 
ventana = tk.Tk()
ventana.title("Gráficas COVID-19 Colombia")
ventana.geometry("800x600")

ancho_boton = 30
alto_boton = 10
colores = [
    "#4CAF50",  # Verde
    "#2196F3",  # Azul
    "#FF9800",  # Naranja
    "#9C27B0",  # Púrpura
    "#F44336",  # Rojo
    "#607D8B",   # Gris azulado
    "#FF9800",  # Naranja
]


frame_botones = tk.Frame(ventana)
frame_botones.grid(row=0, column=0, padx=20, pady=20)


botones = [
    ("Casos por Edad", grafico_por_edad, colores[0]),
    ("Casos por Departamento", grafico_por_departamento, colores[1]),
    ("Casos por Sexo", grafico_por_sexo, colores[2]),
    ("Porcentaje Recuperados", porcentaje_recuperados, colores[3]),
    ("Evolución Diagnósticos", evolucion_por_fecha, colores[4]),
    ("Por Tipo de Contagio", grafico_por_tipo_contagio, colores[5]),
    ("Motrar estadisticas", abrir_ventana_departamento, colores[6])
]


for i, (texto, funcion, color) in enumerate(botones):
    fila = i // 3
    columna = i % 3
    boton = tk.Button(
        frame_botones,
        text=texto,
        command=funcion,
        bg=color,
        fg="white",
        width=ancho_boton,
        height=alto_boton,
        font=("Arial", 10, "bold")
    )
    boton.grid(row=fila, column=columna, padx=10, pady=10)

# Frame para mostrar imagen decorativa
frame_imagen = tk.Frame(ventana)
frame_imagen.grid(row=1, column=0, pady=10)



ventana.mainloop()
