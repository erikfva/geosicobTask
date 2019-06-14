# geosicobTask
Gestor de tareas de geoprocesamiento.
## Configuración
Editar el archivo config.json y configurar los parámetros:

```json
{ 
	"express": { 
		"port": 3000 ###<- Puerto asignado para el servidor.
	}, 
    ### Configuración de la base de datos PostgreSQL.
	"postgres": { 
  	"user": "admderechos",
  	"database": "geodatabase", 
  	"host": "localhost",
  	"port": 5432,
  	"max": 10,
  	"idleTimeoutMillis": 30000 ###<- Tiempo máximo de espera (en milisegundos) para la conexión.
  },
	"PATH_OGR2OGR": "C:\\Program\" Files\"\\\"QGIS 3.2\"\\bin\\ogr2ogr.exe", ###<- Dirección de la herramienta de linea de comando ogr2ogr.
	"PATH_SHP": "C:\\wamp\\www\\gsadmin\\shapefiles\\", ###<- Localización física de la carpeta donde se generarán los archivos comprimidos.
	"URL_SHP": "http://localhost/gsadmin/shapefiles/" ###<- Url que apunta a la carpeta donde se generán los archivos. Debe estar mapeada en el Apache.
```