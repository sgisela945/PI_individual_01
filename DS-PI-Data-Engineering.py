
# LIBRERÍAS UTILIZADAS:
import pandas as pd #para la utilización de dataframe
import numpy as np # para crear array
from base64 import encode #para el encoding de los archivos import encodings
from venv import create#esto creo que no es necesario
from sqlalchemy import create_engine #conexión a MySQL
from IPython.display import HTML, display_html, display#para agregar títulos
from tkinter import Frame #para detectar que el tipo de dato es un dataframe de py
from unicodedata import normalize#para normalizar strings
import re #para normalizar incluyendo la ñ
import os #para desglozar la ruta del archivo
import datefinder #identificar datos tipo objeto/string que son fecha
import datetime #libreria de fechas

#Cargamos el ARCHIVO DE SUCURSAL

#desde un csv
ruta= r'C:\Users\Gise\Desktop\Data Since\LABS\Individuales\Datasets relevamiento precios\sucursal.csv'
sucursal=pd.read_csv(ruta, sep=",", encoding="utf-8")
print("Carga de sucursal completa")


#Cargamos el archivo de prducto

#desde parquet
ruta_parquet=r'C:\Users\Gise\Desktop\Data Since\LABS\Individuales\Datasets relevamiento precios\producto.parquet'
producto= pd.read_parquet(ruta_parquet,engine='auto')
print("Carga de producto completa")

def Estado_general_tabla (df=Frame):

    #Vemos valores null
    nulos=df.isnull().sum().sum()
    #Vemos duplicados y los eliminamos
    duplicados=df.duplicated().sum().sum()
    df.drop_duplicates()

    indice=0
    nombre_columna=[]
    tipo_de_dato=[]
    valores_null=[]
    cantidad_repetidos=[]
    cantidad_valores_unicos=[]
    valores_unicos=[]
    for c in df.columns:
        #vemos si la columna posee un volúmen de datos que sea analizable:
        if df[c].isnull().sum()/(df[c].isnull().sum()+df[c].notnull().sum())>0.7:
            print("Se ha borrado la columna: ",c, "Por falta de datos")
            df.drop(c,axis=1,inplace=True)
        else:
            #Agregamos el nombre de la columna a su lista correspondiente:
            nombre_columna.append(c)
            #detectamos el tipo de dato y lo agregamos a su lista correspondiente:
            tipo_dato=df[c].dtype
            tipo_de_dato.append(tipo_dato)
            
            #Buscamos null y los agregamos a su lista correspondiente:
            nulos=df[c].isnull().sum().sum()
            valores_null.append(nulos)
                            
            #Detectamos las columnas que son string 
            if df[c].dtype == 'object':
                #llenamos valores null
                df[c].fillna("vacio", inplace = True) 
                #ponemos todo mayúsculas en primer palabra y el resto en minúsculas
                df[c]=df[c].str.title() 
                #sacamos los acentos
                for i in df.index: 
                    # -> NFD y eliminar diacríticos
                    s=df[c][i]
                    s = re.sub(r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1", normalize( "NFD", s), 0, re.I)
                    # -> NFC
                    s = normalize( 'NFC', s)
                    df[c][i]=s
                            
            # Buscamos los valores repetidos 
            repetidos=df[c].duplicated().sum()
            #agregamos a la lista de repetidos
            cantidad_repetidos.append(repetidos)
            
            #Buscamos valores diferentes (únicos) que existen:
            array_unicos=df[c].unique()
            #los pasamos a lista para una mejor manipulación
            lista_unicos=array_unicos.ravel().tolist()
            
            #agregamos a la lista cantidad de valores únicos 
            cantidad_valores_unicos.append(len(lista_unicos))
            
            #Agregamos a la lista los valores únicos 
            valores_unicos.append(df[c].unique())
                        
            #Sumamos uno al índice para que continúe recorriendo las columnas
            indice+=1
        
    df_resultado=pd.DataFrame()
    df.style.set_caption('Mi Tabla')#título de la tabla
    df_resultado["Nombre_columna"]=nombre_columna
    df_resultado["Tipo_de_dato"]=tipo_dato
    df_resultado["Cantidad_null"]=valores_null
    df_resultado['Cantidad_repetidos']=cantidad_repetidos
    df_resultado["cantidad_valores_unicos"]=cantidad_valores_unicos
    df_resultado["valores_unicos"]=valores_unicos
    print('Las condiciones iniciales de su tabla son: ')
    print(df_resultado)
    return df

def limpieza_idsucursal (df):
    #separo los id que tienen - y las guardo en una variable
    ver_id=df["id"].str.split('-', expand=True)
    #renombro las columnas para poder usarlas
    ver_id.columns=["comercioid",'banderaid',"idSucursal"]
    #reemplazo por columna id
    df["id"]=ver_id['idSucursal']
    #renombro la columna id
    df.rename(columns={'id': 'idSucursal'}, inplace=True)
    #cambio el tipo de dato a entero
    df['idSucursal'] = df['idSucursal'].astype('int32')
    return df

def limpieza_id_producto (df):
    #reemplazo los valores con -
    df.idProducto.replace("-",regex=True) 

    #Saco los espacios si existen en la columna idProducto
    df['idProducto']=df.idProducto.apply(lambda x: x.strip() if x==str else x)
    #saco las cadenas con más de 20 caracteres por considerarse outliers
    df['idProducto']=df.idProducto.apply(lambda x: 0 if type(x)==str and len(x)>20 else x)
    #sacamos carácteres especiales por si los hubiera
    df['idProducto']=df.idProducto.apply(lambda x: normalize( 'NFC', x) if type(x)==str else x)
    #recorto id a 13 valores
    df['idProducto'] = df['idProducto'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x) 
    #elimino los valores que no pueda manipular
    #df['idProducto'] = df['idProducto'].apply(lambda x: 0 if type(x)==str else x)
    return df

def limpieza_producto (df):
    #LIMPIEZA ID PRODUCTO
    #le cambio el nombre de la columna para que sea descriptiva
    df.rename(columns={'id': 'idProducto'}, inplace=True)
    df=limpieza_id_producto(df)
    #LIMPIEZA NOMBRE DE PRODUCTO
    df.rename(columns={'nombre': 'nombreProducto'}, inplace=True)
    df['nombreProducto']=df['nombreProducto'].apply(lambda x :(" ".join((x.split())[:-2])) if x !=0 else x )
    #papa 1kg--> papa
    return df
    
Estado_general_tabla(sucursal)
limpieza_idsucursal (sucursal)
Estado_general_tabla(producto)
limpieza_producto(producto)

def crear_precio ():
   global precio 
   precio=pd.DataFrame(columns=['año', 'mes', 'día', 'idProducto', 'idSucursal', 'precio', 'comercioId',
      'banderaId', 'banderaDescripcion', 'comercioRazonSocial', 'provincia',
      'localidad', 'direccion', 'lat', 'lng', 'sucursalNombre',
      'sucursalTipo', 'marca', 'nombreProducto', 'presentacion'])
   return precio

#FUNCION PARA LA CARGA INCREMENTAL
def carga_semana (ruta):
    #CARGA DE DATOS Y EXTRACCIÓN DE FECHA:
    #limpio la ruta del atchivo dejando solo el nombre y la extensión
    tupla_archivos=os.path.split(ruta)
    #de la tupla que me devuelve la divido en el nombre que lo voy a usar como suministro de fecha y en extensión
    nombre_tipo_archivo=tupla_archivos[1]
    #la primer parte de la tupla es el nombre por lo que lo guardo en una variable   
    nombre_archivo= os.path.splitext(nombre_tipo_archivo)[0]
    #la segunda parte de la tupla es el tipo de archivo que es que lo uso para saber cómo ingestar
    extension_archivo= os.path.splitext(nombre_tipo_archivo)[-1]
    #detección de la manera de abrirlo en base a la extensión del archivo
    if extension_archivo==".json":
        #desde json
        df=pd.read_json(ruta)
    elif extension_archivo==".csv":
        #desde csv
        try:
            df=pd.read_csv(ruta,sep=",",encoding='utf-16')
        except:
            df=pd.read_json(ruta,sep=",",encoding='auto')
    elif extension_archivo==".txt":
    #desde txt
        df=pd.read_csv(ruta,sep="|",encoding='utf-8')
    elif extension_archivo==".xlsx":
        #desde un excel
        df=pd.read_excel(ruta,header=0)
        
        #Normalizamos
        df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
        df['sucursal_id'] = df['sucursal_id'].apply(lambda x: ('{0}-{1}-{2}'.format(int(x.day),int(x.month),int(x.year))) if(type(x) == datetime.datetime) else x)
        df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
        df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
        
    #MODIFICACIÓN DEL DATO:
    #agrego la columna año y semana
    df['año']=int(nombre_archivo[-8:-4])
    df['mes']=int(nombre_archivo[-4:-2])
    df['día']=int(nombre_archivo[-2:len(nombre_archivo)+1])
    #renombro las columnas
    df.rename(columns={ 'producto_id':'idProducto', 'sucursal_id':'idSucursal'},inplace=True)
    #reordeno las columnas  
    df=df.reindex(columns=['año','mes','día','idProducto','idSucursal','precio'])

    return df
    
def limpieza_semana (df=Frame):
    #le hago las limpiezas pertinentes:
    #borramos los valores null debido a que no nos sirve no tener el dato completo
    df.dropna(how='any',inplace= True)
    
    #Borramos duplicados 
    df.drop_duplicates(inplace= True)

    #Saco los espacios
    df.columns = df.columns.str.replace(' ', '')
    
    #Normalización idProducto
    df=limpieza_id_producto(df)

    #Normalización idSucursal:
    #separo los id que tienen - y las guardo en una variable
    ver_id=df['idSucursal'].str.split('-', expand=True)
    #renombro las columnas para poder usarlas
    ver_id.columns=["comercioid",'banderaid',"idSucursal"]
    df['idSucursal']=ver_id['idSucursal']
    
    #Normalización de columna precio
    #Saco los espacios si existen en la columna precio
    df['precio']=df.precio.apply(lambda x: x.strip() if x==str else x)
    #saco las cadenas con más de 20 caracteres por considerarse outliers
    df['precio']=df.precio.apply(lambda x: 0 if type(x)==str and len(x)>20 else x)
    #sacamos carácteres especiales por si los hubiera
    df['precio']=df.precio.apply(lambda x: normalize( 'NFC', x) if type(x)==str else x)
    #elimino los valores que no pueda manipular
    df['precio'] = df['precio'].apply(lambda x: 0 if type(x)==str else x)    
    
    #Cambio el tipo de dato
    df['año'] = df['año'].astype('int32')
    df['mes'] =df['mes'].astype('int32')
    df['día'] = df['día'].astype('int32')
    #df['idProducto'] = df['idProducto'].astype('int32') no llegamos pero lo dejamos para que lo hagan ustedes en el futuro
    df['idSucursal'] = df['idSucursal'].astype('int32')
    df['precio'] = df['precio'].astype('float64')
    
    #combino con la tabla de sucursal y producto y luego actualizo la de precios --> lo convierto en una única tabla!
    df=df.merge(sucursal, on='idSucursal',how='left')
    df=df.merge(producto, on='idProducto',how='left')

    return(df)

def actualizar_cargar_precios (ruta):
    #ruta=input("Inserte la ruta COMPLETA de su archivo de precios")--> para que expandan el proyecto deben modificar un poco esta función pero depende de la necesidad
    #ruta=concat
    df=carga_semana(ruta)
    df=limpieza_semana(df)
    global last_price
    last_price=pd.concat([last_price,df])
    #eliminamos duplicaodos
    last_price.drop_duplicates(inplace=True) 
    return(last_price)   
#creamos un df vacío
last_price=pd.DataFrame()
#le cargamos todas las tablas de semana que tenemos disponibles
last_price=actualizar_cargar_precios (r'C:\Users\Gise\Desktop\Data Since\LABS\Individuales\Datasets relevamiento precios\precios_semana_20200413.csv')
last_price=actualizar_cargar_precios(r'C:\Users\Gise\Desktop\Data Since\LABS\Individuales\Datasets relevamiento precios\precios_semana_20200503.json')
last_price=actualizar_cargar_precios(r'C:\Users\Gise\Desktop\Data Since\LABS\Individuales\Datasets relevamiento precios\precios_semana_20200518.txt')
last_price=actualizar_cargar_precios(r'C:\Users\Gise\Desktop\Data Since\LABS\Individuales\Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx')
print(last_price)

#ENVIAMOS LAS TABLAS A MYSQL

from sqlalchemy import create_engine
import pyodbc
import os
#creamos conexión con MySQL
cadena_conexion="mysql+pymysql://root@127.0.0.1:3306/labs_inidividual_01"
conexion=create_engine(cadena_conexion)

#pasamos a mysql la tabla de precios
last_price.to_sql(name="precios",con=conexion,if_exists="replace",index=False)
#pasamos a mysql la tabla de sucursal
sucursal.to_sql(name="sucursal",con=conexion,if_exists="replace",index=False)
#pasamos a mysql a tabla de producto
producto.to_sql(name="producto",con=conexion,if_exists="replace",index=False)

#Creamos copias de respaldo de las tablas limpias
#para precio
last_price.to_csv('precio_limpio.csv')
#para producto
producto.to_csv('producto_limpio.csv')
#para sucursal
sucursal.to_csv('sucursal_limpio.csv')

#creamos la opción de insertar ruta desde un input
def actualizar_cargar_precios_usuario ():
    ruta=str(input("Inserte la ruta COMPLETA de su archivo de precios"))
    df=carga_semana(ruta)
    df=limpieza_semana(df)
    global last_price
    last_price=pd.concat([last_price,df])
    #eliminamos duplicaodos
    last_price.drop_duplicates(inplace=True) 
    #preguntamos si quiere ingresar más archivos
    answ=int(input("ingrese uno si quiere seguir cargando datos"))
    if answ!=1:
        #pasamos a mysql la tabla de precios
        last_price.to_sql(name="precios",con=conexion,if_exists="replace",index=False)
    else:
        last_price=actualizar_cargar_precios_usuario()
    return(last_price) 

last_price=actualizar_cargar_precios_usuario()
