# PI_individual_01
MySQL Python Power Bi Anaconda Visual Studio Code GIT GitHub Terminal

Hola a todos! 👋👩‍💻 Soy Gisela 
💻 console.log ("Hola mundo")🌎 

### Proyecto:Data Engineering
 10.2022 || PROCESO DE CARGA DE DATOS A UNA BASE DE DATOS CON MOTOR SQL
 
📌Archivos en la parte superior: 
En este primer proyecto realizo un proceso de ETL (extract, transform and load) a partir de un conjunto de datos que se enfocarán en una misma perspectiva de negocio. Los datos vienen de diversas fuentes de relevamiento de precios en distintos mercados. Debí trabajar los diferentes tipos de archivos para llevarlos a una misma extensión y, una vez finalizada esta etapa, cree los joins necesarios con el objetivo de crear un DER y dejarlos almacenados en un archivo con extensión .db. Por último, todo el trabajo deberá contemplar la carga incremental del archivo "precios_semana_20200518.txt".


### Pasos seguidos:
Procesar los diferentes datasets.
Crear un archivo DB con el motor de SQL que quieran. Pueden usar SQLAlchemy por ejemplo.
Realizar en draw.io un diagrama de flujo de trabajo del ETL y explicarlo en vivo (ver enlace del video a continuación).
Realizar una carga incremental de los archivos que se tienen durante el video.
Realizar una query en el video, para comprobar el funcionamiento de todo su trabajo. La query a armar es la siguiente: Precio promedio de la sucursal 688.
![Pipline](https://user-images.githubusercontent.com/104787036/198356565-ee2bf555-d960-470f-a9f7-4cd7bddd4a41.JPG)


### Glosario del trabajo:
      Diccionario de datos
      Precio: Precio del producto en pesos argentinos.
      Vpn_keyproducto_id: Código EAN del producto.
      Text_formatsucursal_id: ID de la sucursal. Los dos primeros números determinan la cadena.
      Productos.parquet  

      id: Código EAN del producto.
      marca: Marca del producto.
      nombre: Detalle del producto.
      Presentación: Unidad de presentación del producto.
      categoria1: categoria a la que pertenece el producto.
      categoria2: categoria a la que pertenece el producto.
      categoria3: categoria a la que pertenece el producto.
      Sucursales.csv  

      Id: Es el ID principal, compuesto por la concatenación de banderaId-comercioId-sucursalId.
      comercioId: es el identificador de la empresa controlante. Por ejemplo Hipermecado Carrefour y Express tienen mismo comercioId pero distinta banderaId
      banderaId: Diferencia las distintas "cadenas" de la misma empresa. Vea, Disco y jumbo pertenecen al mismo comercio pero tienen distinta bandera.
      banderaDescripcion: Nombre de fantasía de la Cadena
      comercioRazonSocial: Nombre con la que está registrada legalmente la empresa.
      provincia: Provincia donde está ubicado el comercio.
      localidadl: Localidad donde está ubicado el comercio.
      direccion: Dirección del comercio,
      lat: Coordenadas de latitud de la ubicación del comercio
      lng: Coordenadas de longitud de la ubicación del comercio

### Tips utilizados:
  Analizar en que instancia de su ETL realizar la limpieza y joins de los datasets.
  Comenté el código en todo momento.

VIDEO DEMO
El video no dura más de 5 minutos, en el mismo, muestro y explico el proceso de extracción, transformación y carga de los datos, fundamentación de las herramientas elegidas en conjunto a la demora del trabajo. Por otra parte, expone la carga incremental asignada.
	:sunglasses: 
	
	📌Enlace :https://youtu.be/1oCWx4F9Tr4

 👩‍💻 Muchas gracias y Saludos!
