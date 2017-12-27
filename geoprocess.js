const config = require('./config.json');

const express = require('express')
const app = express()
const shell = require('shelljs')
const pg = require('pg')
//Para compresion zip:
var fs = require('fs');
var archiver = require('archiver');


//process.env.APPDATA = '' //Path donde se encuentra la careta postgresql que contiene el archivo pgpass.conf

	const pool = new pg.Pool(config.postgres)
  pool.on('error', function (err, client) {
    // if an error is encountered by a client while it sits idle in the pool
    // the pool itself will emit an error event with both the error and
    // the client which emitted the original error
    // this is a rare occurrence but can happen if there is a network partition
    // between your application and the database, the database restarts, etc.
    // and so you might want to handle it and at least log it out
    console.error('idle client error', err.message, err.stack)
  })


app.get('/', function (req, res) {
  res.send('Bienvenido al servidor de geoprocesamiento!')
})


app.get('/do/:id', (req, res, next) => {

  // to run a query we can acquire a client from the pool,
  // run a query on the client, and then return the client to the pool
  var idgeoproceso = isNaN(parseInt(req.params.id))?-1:parseInt(req.params.id);

  pool.connect(function(err, client, done) {
    if(err) {
      console.error('error fetching client from pool', err);
      return res.status(500).json({success: false, msg: err.message});
    }

    client.query('SELECT idusuario, proceso, entrada, salida FROM registro_derecho.geoprocesamiento WHERE idgeoproceso =' + parseInt(idgeoproceso) , function(err, result) {
      //call `done(err)` to release the client back to the pool (or destroy it if there is an error)
      done(err);

      if(err) {
        console.error('error running query', err);
        return res.status(500).json({success: 0, msg: 'Error: ' + err.message});
      }
      if(!result.rows.length){
      	return res.status(500).json({success: 0, msg: 'Error: No se ha encontrado el proceso ' + idgeoproceso});
      }

      console.log('Proceso: ' + result.rows[0].proceso);
      result.rows[0].entrada.idgeoproceso = idgeoproceso
      switch (result.rows[0].proceso) {
      	case 'sicob_build_shapefiles':
      		//Registrando inicio del proceso en el log.
      		sicob_log_geoproceso('{"idgeoproceso":"' + idgeoproceso + '", "exec_point":"inicio"}',function(err, res){
      			if(err){
      				console.log('sicob_build_shapefiles, idgeoproceso=' + idgeoproceso + ': Error al registrar el log inicial!!! ' + err)
      			}
      		})
      		//Ejecutando la funcion.
      		sicob_build_shapefiles(result.rows[0].entrada,function(err,resJSON){
      			if(err){
      				console.log('sicob_build_shapefiles(' + idgeoproceso + '): Al final ocurrio este error: ' + err)
      			}
      			//Registrando fin del proceso en el log.
      			sicob_log_geoproceso('{"success":"1","idgeoproceso":"' + idgeoproceso + '", "exec_point":"fin","payload":' + JSON.stringify(resJSON) + '}',function(err, res){
      				if(err){
      					console.log('sicob_build_shapefiles(' + idgeoproceso + '): Error al registrar el log final!!! ' + err)
      				}
      			})
      		})
      		break
      	case 'sicob_analisis_sobreposicion':
      		sicob_log_geoproceso('{"idgeoproceso":"' + idgeoproceso + '", "exec_point":"inicio"}',function(err, res){
      			if(err){
      				console.log('sicob_analisis_sobreposicion(' + idgeoproceso + '): Error al registrar el log inicial!!! ' + err)
      			}
      		})

      		sicob_analisis_sobreposicion(result.rows[0].entrada, function(err,resJSON){
      			if(err){
      				console.log('sicob_analisis_sobreposicion(' + idgeoproceso + '): Al final ocurrio este error: ' + err)
      				resJSON.error = err.message
      			}

      			sicob_log_geoproceso('{"success":"1","idgeoproceso":"' + idgeoproceso + '", "exec_point":"fin","payload":' + JSON.stringify(resJSON) + '}',function(err, res){
      				if(err){
      					console.log('sicob_analisis_sobreposicion(' + idgeoproceso + '): Error al registrar el log final!!! ' + err)
      				}
      			})
      			//console.log(resJSON)
      			console.log('sicob_analisis_sobreposicion(' + idgeoproceso + '): Termino.')
      		})
      		break
      	default:
      		sicob_rungeoprocess(idgeoproceso, function(err,resJSON){
      			if(err){
      				console.log('sicob_rungeoprocess(' + idgeoproceso + '): Al final ocurrio este error: ' + err)
      			}
      			console.log('sicob_rungeoprocess(' + idgeoproceso + '): Termino.')
      		});
      }
      return res.status(200).json({success: true, msg: "Ejecutando geoproceso...", "idgeoproceso": idgeoproceso});
    });

  });

});

app.listen(config.express.port, function () {
  console.log('Servicios de geoprocesamiento disponibles en el puerto ' + config.express.port + '!')
})

function executeScalar(sql, cb){
	var sql = 'SELECT t.* FROM (' + sql + ') t';
	executeSQL(sql,function(err, res){
		 if (err) {
    	if(cb) cb(err, null)
    	return;
  	}
  	//console.log('ejecutado ' +  sql)
   	if(cb) cb(null,res.rows[0][Object.keys(res.rows[0])])
  })
}

function executeSQL(sql,cb){
  // to run a query we can acquire a client from the pool,
  // run a query on the client, and then return the client to the pool

  pool.query(sql, (err, res) => {
  	if (err) {
    	if(cb) cb(err, null)
    	return;
  	}
		//console.log(res)
  	//console.log('ejecutado ' +  sql)
   	if(cb) cb(null, res)
	})
}

function batchSQL(sqlList,cb,finish){

	var sqlPending = sqlList.length
	var execNext = function(){
		var sql = sqlList.shift()
		console.log('ejecutando...' + sql)
		 (sql,function(err,rows){
			sqlPending--
			if (cb) cb(err, rows, sql)
			if(sqlPending == 0){
				if(finish) finish(rows)
      	return
			}
		})
		if (sqlList.length) execNext();
  }
  execNext()
}

function execute(cmd,cb, envvar ){
	var child = shell.exec(
		cmd,
		{async:true, silent:true, detached: true, env: envvar||{}, stdio: 'ignore'/*, encoding : 'latin1'*/},
		function(code, stdout, stderr) {
			if(code){
				console.log(stderr)
			}
        if (cb) cb(code, stdout, stderr)
		}
	);
	child.unref()
}

function batch(cmds,cb,finish){
	var execCount = cmds.length
	var execNext = function(){
		var cmd = cmds.shift()
        execute(cmd, function(code, stdout, stderr ){
            execCount = execCount -1
            if (cb) cb(cmd,code, stdout, stderr)
            if(execCount == 0){
            	if(finish) finish(stdout)
            	return
            }
        });
        if (cmds.length) execNext();
  };
  execNext();
}

function sicob_rungeoprocess(idgeoproceso, cb ){
	var sql = 'SELECT sicob_ejecutar_geoproceso(\'{""idgeoproceso"":""' + parseInt(idgeoproceso) + '""}\'::json) AS _out';
	var cmd = 'psql -d geodatabase -U admderechos -c "' + sql + '"';

	//console.log(cmd);

	//var child = exec(cmd,
	//{async:true, silent:true, detached: true,  stdio: 'ignore'/*, encoding : 'latin1'*/});
	//child.unref();
	execute(cmd,function(code, stdout, stderr){
		if (cb)	cb(code?stderr:null, stdout)
	})
}


function buildWHERE(condition){
	var sqlConditionBuilder = require("sql-condition-builder")
	var builder = new sqlConditionBuilder()
	return  builder.build(condition)
}

/*
	-Datos de entrada:
	lyr_list --> Array JSON donde cada elemento tiene los sig. atributos:
		lyr --> Nombre de la capa en la geodatabase, ej: "processed.f20170704efgdcab41a98021_nsi".
		fname --> Nombre para el archivo resultante. Este nombre se convierte a minusculas y se le adiciona
							el subfijo _geo_geosicob, ej. "cApa3" producira el archivo "capa3_geo_geosicob.shp".
	  condition --> Filtro para los datos de la capa.

	{"lyr_list": [{"lyr": "processed.f20170704efgdcab41a98021_nsi", "fname": "Capa1", "condition":  {"nombre": "Poligono2"} }, {"lyr": "processed.f20170704efgdcab41a98021_nsi8", "fname": "cApa2", "condition": {}}, {"lyr": "uploads.f20170704efgdcab41a98021", "fname": "capa3" }]}
*/
function sicob_build_shapefiles(data,cb){
	//console.log(data);

	var now = new Date();

	var timesid = now.getFullYear().toString(); // 2011
	timesid += (now.getMonth() < 9 ? '0' : '') + (now.getMonth()+1).toString(); // JS months are 0-based, so +1 and pad with 0's
	timesid += (now.getDate() < 10 ? '0' : '') + now.getDate().toString(); // pad with a 0
	timesid += (now.getHours() < 10 ? '0' : '') + now.getHours().toString(); // pad with a 0
	timesid += (now.getMinutes() < 10 ? '0' : '') + now.getMinutes().toString(); // pad with a 0
	timesid += (now.getSeconds() < 10 ? '0' : '') + now.getSeconds().toString(); // pad with a 0
	timesid += (now.getMilliseconds() < 10 ? '00' : (now.getMilliseconds() < 100 ? '0' :'')) + now.getMilliseconds().toString(); // pad with a 0

	var fzipname = 'sicob_shp' + timesid
	var cmds = []
	shell.mkdir('-p', config.PATH_SHP + timesid)

	data.lyr_list.forEach(function (lyr) {
		console.log(lyr)
		let _condition = lyr.condition || {}
		let condition = Object.keys(_condition).length === 0 ? 'TRUE' : buildWHERE(_condition)
		console.log(condition)

		var cmd = config.PATH_OGR2OGR + ' -f "ESRI Shapefile" ' + config.PATH_SHP + timesid + '/' + (lyr.fname || lyr.lyr).toLowerCase() + '_geo_geosicob.shp PG:"host=' + config.postgres.host + ' user=' + config.postgres.user + ' password=\'\'' + ' dbname=' + config.postgres.database + '" -geomfield the_geom -where "' + condition + '" -overwrite --config PG_USE_COPY YES "' + lyr.lyr + '"'
		cmds.push(cmd)
	})
	var files_cnt = data.lyr_list.length
	var error = new Error('')

	batch(
		cmds,
		function(cmd, code, stdout, stderr){
			console.log('(sicob_build_shapefiles) ejecuto...',cmd)
			if(code){
				files_cnt--
				console.log('Error (sicob_build_shapefiles): ' + stderr)
				error.message = error.message==''? 'Error (sicob_build_shapefiles/zip):' + stderr : error.message + ', ' + stderr
			}
		},
		function(){
			console.log('(sicob_build_shapefiles)...termino los shp.')
			zip({
				globpatt : '{*.shp,*.dbf,*.shx,*.prj}', //Patron usado por node-glob para generalizar nombres de archivos.
				ftarget : config.PATH_SHP + fzipname + '.zip', //Nombre del archivo .zip generado, incluyendo la direcci�n de carpeta.
				workdir : config.PATH_SHP + timesid + '/', //(opcional) Directorio donde se encuentran los archivos a comprimir.
				cb : function(err){ //(opcional) Funci�n de callback que se ejecuta al finalizar el trabajo.
					var result = {link : config.URL_SHP + fzipname + '.zip', shp_cnt : files_cnt, success : 1}
					execute('rmdir /S /Q "' + config.PATH_SHP + timesid + '"');
					if(err){
						error.message = error.message==''? 'Error (sicob_build_shapefiles/zip):' + err : error.message + ', ' + err
						console.log('Error (sicob_build_shapefiles/zip): ' + err)
					}

					if(error.message){
						result.success = 0
						result.error = error.message.replace(/\r|\n/g,"")
					}

					if (cb)	cb(error.message?error.message:null, result)
				}
			})
		}
	)
}

function sicob_analisis_sobreposicion(op,cb){
	var doanalisys = op.doanalisys || []
	var listSQL = []

	if(doanalisys.length == 0){ //Si no se especifica el tipo de analisis entonces es un full analisis.
		op.doanalisys = ["ATE", "ASL", "PGMF", "POAF"];
		listSQL.push('SELECT sicob_analisis_sobreposicion(\'' + JSON.stringify(op) + '\')');
		op.doanalisys = ["POP", "PDM", "RF"];
		listSQL.push('SELECT sicob_analisis_sobreposicion(\'' + JSON.stringify(op) + '\')');
		op.doanalisys = ["RPPN", "TPFP", "PLUS"];
		listSQL.push('SELECT sicob_analisis_sobreposicion(\'' + JSON.stringify(op) + '\')');
		op.doanalisys = ["D337", "DPAS", "APN", "APM"];
		listSQL.push('SELECT sicob_analisis_sobreposicion(\'' + JSON.stringify(op) + '\')');
	}

	var resJSON = {}
	var error = new Error('')

	batchSQL(
		listSQL,
		function(err, rows, sql){
			//console.log('Se ejecut�...',sql)
			if(err){
				error.message = error.message==''? err.message: error.message + ', ' + err.message
				console.log('Error: ' + err)
				return
			}
			//console.log(rows)
			resJSON = Object.assign(resJSON, rows[0].sicob_analisis_sobreposicion)
		},
		function(rows){
			console.log('Termino!!!')
			return cb(error.message==''?null:error.message,resJSON)
			//return res.json(resJSON)//rows[0])
		}
	)
}

function sicob_log_geoproceso(strOp, cb){
	var sql = 'SELECT sicob_log_geoproceso( (\''+ strOp.replace(/\'/g, "''''") +'\')::json )';
  executeSQL(sql,function(err, res){
		if(cb) cb(err,res)
  })
}

function sicob_overlap(params){
	return new Promise(function(resolve, reject) {
		var sql = 'SELECT sicob_overlap(\'' + JSON.stringify(params) + '\')'
		executeScalar(sql,function(err, res){
			if(err){
				console.log(sql)
				throw err
			}
			return resolve(res)
		})
	})
}

/*** FUNCION PARA COMPRIMIR ARCHIVOS EN FORMATO ZIP ***/
/*	Variables de op:
				globpatt : '{*.shp,*.dbf,*.shx,*.prj}', //Patron usado por node-glob para generalizar nombres de archivos.
				ftarget : PATH_SHP + fzipname + '.zip', //Nombre del archivo .zip generado, incluyendo la direcci�n de carpeta.
				workdir : PATH_SHP + timesid + '/', //(opcional) Directorio donde se encuentran los archivos a comprimir.
				cb : function(err){...} //(opcional) Funci�n de callback que se ejecuta al finalizar el trabajo.
*/
function zip(op){
  // create a file to stream archive data to.

	var output = fs.createWriteStream(op.ftarget)

	output.on("error", (err) => {
		output.end();
		console.log('error al crear archivo!!! ')
		if(op.cb){
  		op.cb(err)
  		return
  	}
  	console.log(err)
  	return
  })

  var archive = archiver('zip', {
      zlib: { level: 9 } // Sets the compression level.
  })

  // listen for all archive data to be written
  output.on('close', function() {
    console.log(archive.pointer() + ' total bytes');
    console.log('archiver has been finalized and the output file descriptor has closed.');
    if(op.cb){ op.cb(null) }
  })

  // good practice to catch warnings (ie stat failures and other non-blocking errors)
  archive.on('warning', function(err) {
  	if(op.cb){
  		op.cb(err)
  		return
  	}
    if (err.code === 'ENOENT') {
        // log warning
    } else {
        throw error
    }
  })

  // good practice to catch this error explicitly
  archive.on('error', function(err) {
  	if(op.cb){
  		op.cb(err)
  		return
  	}
    throw err;
  })

  // pipe archive data to the file
  archive.pipe(output)


  // append files from a glob pattern
  archive.glob(op.globpatt,{ nodir: true, nomount   : true, cwd : op.workdir || process.cwd() })
  // finalize the archive (ie we are done appending files but streams have to finish yet)
  archive.finalize();
}

function sicob_obtener_predio(op,cb){
	var _tolerance = op.tolerance || '5.3'
	var tbl_name = op.lyr_in.indexOf('.') !== -1 ? (op.lyr_in.split('.'))[1] : op.lyr_in
	var _condition = op.condition || 'TRUE'
	var _fldpredio = op.fldpredio_parc || 'predio'
	var _fldpropietario = op.fldpropietario_parc || 'propietario'
	var a = op.lyr_in
	var lyr_predios = 'temp.' + tbl_name + '_pred' //nombre para la capa de predios resultante.
	var lyr_parcelas = 'temp.' + tbl_name + '_ppred' //nombre para la capa de parcelas resultante.
	var createdResult = false //variable que indica si ya se ha creado la tabla de la capa resultado.

	var lyrs_predio = [{
    	"subfix":"_tit",
        "lyr_parc":{
        	"source":"coberturas.parcelas_tituladas",
            "fldidpredio":"idpredio",
            "fldpredio":"predio",
            "fldpropietario":"propietario"
        },
        "lyr_pred":{
        	"source":"coberturas.predios_titulados",
            "fldidpredio":"idpredio"
        }
    },
	{
    	"subfix":"_tioc",
        "tolerance":"0",
        "lyr_parc":{
        	"source":"coberturas.tioc",
            "fldidpredio":"idpredio",
            "fldpredio":"nomparcela",
            "fldpropietario":"nomparcela"
        },
        "lyr_pred":{
        	"source":"coberturas.predios_tioc",
            "fldidpredio":"idpredio"
        }
    },
	{
    	"subfix":"_pop",
        "tolerance":"0",
        "lyr_parc":{
        	"source":"coberturas.predios_pop",
            "fldpredio":"nom_pre",
            "fldpropietario":"nom_pro"
        }
    },
	{
    	"subfix":"_proc",
        "tolerance":"0",
        "lyr_parc":{
        	"source":"coberturas.predios_proceso_geosicob_geo_201607",
            "fldidpredio":"idpredio",
            "fldpredio":"nompred",
            "fldpropietario":"beneficiar"
        },
        "lyr_pred":{
        	"source":"coberturas.predios_referenciales",
            "fldidpredio":"sicob_id"
        }
	}]

	const do_configurar_capa_parcelas = new Promise(function(resolve, reject) {
			if ((op.lyr_parc || '') == '' ) return resolve()
			//--> Si se indica cobertura de referencia.
			var sql = `SELECT json_build_object('exist_predio',sicob_exist_column('${op.lyr_parc}', '${_fldpredio}'),
				'exist_propietario',sicob_exist_column('${op.lyr_parc}', '${_fldpropietario}'))`
	  	executeScalar(sql,function(err, res){
				if(err){
					return reject(err)
				}
				_fldpredio = res.exist_predio?_fldpredio:"'COLUMNA NO ENCONTRADA'"
				_fldpropietario = res.exist_propietario?_fldpropietario:"'COLUMNA NO ENCONTRADA'"
				lyrs_predio.unshift({
	                        "lyr_parc":{"source": op.lyr_parc,
	                        "fldpredio": _fldpredio,
	                        "fldpropietario": _fldpropietario},
	                        "subfix":"_parc"})
				resolve()
	  	})
	})

	const do_actualizar_referencia = function(sourceA, sourceAB, overlaying){
		return new Promise(function(resolve, reject) {
			if (sourceAB == op.lyr_in) return resolve()
			//-->Cambiando la referencia "id_a" de "a" hacia la tabla de entrada "_lyr_in"
			sql = `
			UPDATE
				 ` + overlaying.lyr_over + ` a
			SET
				id_a = (
					SELECT
						b.id_a
					FROM
						` + sourceAB + ` b
					WHERE
						b.sicob_id = a.id_a
					LIMIT 1
				), source_a = '` + sourceA + `'
			`
			executeSQL(sql,function(err,res){
				if(err) return reject(err)
				return resolve()
			})
		})
	}

	const do_add_titulacion = function(lyr,condition){
		return new Promise(function(resolve, reject) {
			console.log('localizando info de titulacion...')
			var params = {
				"a": lyr,
				"condition_a": condition || '',
				"b":"coberturas.parcelas_tituladas","subfix":"_tit","tolerance":"0","add_diff":false
			}

			var tmp_overlaying = sicob_overlap(params)
			tmp_overlaying.then(function(res){

				if(parseInt(res.features_inters_cnt) == 0) return resolve()
				// --> Si se han encontrado parcelas tituladas.
				var sql = `
					UPDATE ${lyr} a
					SET titulo = b.titulo, fecha_titulo = b.fecha_titulo, tipo_propiedad = b.tipo_propiedad, propietario_titulado = b.propietario
					FROM (
						SELECT id_a,
							min(idpredio) as idpredio,
							string_agg(predio , ',') as predio,
							string_agg(propietario , ',') as propietario,
							string_agg(titulo , ',') as titulo,
							string_agg(to_char(fecha_titulo,'DD/MM/YYYY') , ',') as fecha_titulo,
							min(tipo_propiedad) as tipo_propiedad,
							sum(sup_predio) as sup_predio
						FROM
							${res.lyr_over}
						GROUP BY id_a
					) b
					WHERE
					b.id_a = a.sicob_id
				`

				executeSQL(sql,function(err, res){
					if(err){
						console.log(sql)
						throw err
					}
					resolve(res)
				})

			})
		})
	}

	const do_add_pop = function(lyr,condition){
		return new Promise(function(resolve, reject) {
			console.log('localizando info de POP...')
			var params = {
				"a": lyr,
				"condition_a": condition || '',
				"b":"coberturas.predios_pop","subfix":"_pop" + ( Math.floor((Math.random() * 100) + 1) ) ,"tolerance":"0","add_diff":false
			}

			var tmp_overlaying = sicob_overlap(params)
			tmp_overlaying.then(function(res){

				if(parseInt(res.features_inters_cnt) == 0) return resolve()
				// --> Si se han encontrado parcelas con POP.
				var sql = `
            UPDATE ${lyr} a
            SET resol_pop = b.res_adm, fec_resol_pop = b.fec_res
            FROM ${res.lyr_over} b
            WHERE
            b.id_a = a.sicob_id
				`
				executeSQL(sql,function(err, res){
					if(err){
						console.log(sql)
						throw err
					}
					resolve(res)
				})

			})
		})
	}

	const do_add_ubicacion = function(lyr,condition){
		return new Promise(function(resolve, reject) {
			console.log('adicionando info de ubicacion politico-administrativa...')

			var sql = `
          UPDATE ${lyr} a
          SET nom_dep = b.nom_dep,nom_prov = b.nom_prov, nom_mun = b.nom_mun
          FROM (SELECT * FROM sicob_ubication('${lyr}','${condition}') ) b
          WHERE
          b.sicob_id = a.sicob_id
			`
			//console.log(sql)
			executeSQL(sql,function(err, res){
				if(err){
					console.log(sql)
					throw err
				}
				resolve(res)
			})

		})
	}
	const do_registrar_resultado = function(lyr_predio_conf,lyr_overlaying,condition){
		return new Promise(function(resolve, reject) {
			//if(parseInt(overlaying.features_inters_cnt) == 0) return resolve() //Si no existen poligonos para adicionar termina la tarea.

			var subfix = lyr_predio_conf.subfix
			var _fldpredio = (lyr_predio_conf.lyr_parc? lyr_predio_conf.lyr_parc.fldpredio: null) || 'predio'
			var _fldpropietario = ( lyr_predio_conf.lyr_parc? lyr_predio_conf.lyr_parc.fldpropietario: null) || 'propietario'
			var _fldtitulo = 'CAST(NULL as text)'
			var _fldpropietario_titulado = 'CAST(NULL as text)'
			var _fldfecha_titulo = 'CAST(NULL as text)'
			var _fldtipo_propiedad ='CAST(NULL AS text)'
			var _fldsup_predio = 'CAST(NULL AS float)'
			var _fldparcela = 'CAST(NULL AS text)'
			var _fldresol_pop = 'CAST(NULL AS text)'
			var _fldfec_resol_pop = 'CAST(NULL AS text)'
			var _fldsource_predio = (lyr_predio_conf.lyr_pred? lyr_predio_conf.lyr_pred.source: null) || (lyr_predio_conf.lyr_parc? lyr_predio_conf.lyr_parc.source:null) || ''
			var _fldid_predio = (lyr_predio_conf.lyr_parc?lyr_predio_conf.lyr_parc.fldidpredio:null) || 'id_b'
			var __condition = condition || 'id_b IS NOT NULL'
			switch(subfix){
				case '_parc':
					_fldsup_predio = 'sicob_sup'
					_fldparcela = _fldpredio
					break
				case '_tit':
					_fldtitulo = 'titulo'
					_fldfecha_titulo = 'to_char(fecha_titulo,\'DD/MM/YYYY\')'
					_fldpropietario_titulado = 'propietario'
					_fldtipo_propiedad = 'tipo_propiedad'
					_fldsup_predio = 'sup_predio'
					break
				case '_tioc':
					_fldtipo_propiedad = 'CAST(\'Territorio Indígena Originario Campesino\' as text)'
					break
				case '_pop':
					_fldsup_predio = 'CASE sup_pre > 0 WHEN TRUE THEN sup_pre ELSE sicob_sup END'
					_fldresol_pop = 'res_adm'
					_fldfec_resol_pop = 'to_char(fec_res,\'DD/MM/YYYY\')'
					break
				case '_proc':
					_fldsup_predio = 'sicob_sup'
					break
				default:
			}

			var sql = `
							SELECT
								${_fldparcela}::text AS parcela,
								${_fldpredio}::text as predio,
								${_fldpropietario}::text as propietario,
								${_fldtitulo} as titulo,
								${_fldfecha_titulo} AS fecha_titulo,
								${_fldpropietario_titulado} AS propietario_titulado,
								${_fldtipo_propiedad} AS tipo_propiedad,
								${_fldresol_pop} AS resol_pop,
								${_fldfec_resol_pop} AS fec_resol_pop,
								${_fldsup_predio} AS sup_predio,
								CAST(NULL AS text) AS nom_dep, CAST(NULL AS text) nom_prov, CAST(NULL AS text) nom_mun,
								id_a,
								source_a,
								id_b,
								source_b as source_parcela,
								CAST('${_fldsource_predio}' AS text) AS source_predio,
								${_fldid_predio} AS id_predio,
								SICOB_utmzone_wgs84(the_geom) AS sicob_utm,
								round((ST_Area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)))/10000)::numeric,5) as sicob_sup,
								the_geom
							FROM
								${lyr_overlaying}
							WHERE (${__condition})
							AND round((ST_Area(ST_Transform(the_geom, SICOB_utmzone_wgs84(the_geom)))/10000)::numeric,5) > 0.002
							`
			sql = createdResult?
				'INSERT INTO ' + lyr_parcelas + ' ' + sql
				: `DROP TABLE IF EXISTS ${lyr_parcelas};
					CREATE TABLE ${lyr_parcelas} AS ${sql};
					ALTER TABLE  ${lyr_parcelas} ADD COLUMN sicob_id SERIAL PRIMARY KEY;
					CREATE INDEX ${tbl_name}_ppred_geomidx ON ${lyr_parcelas}	USING GIST (the_geom);`
			createdResult = true

			executeSQL(sql,function(err, res){
				if(err)	{
					console.log('do_registrar_resultado: Error ejecutando -> ' + sql, err)
					throw err
				}

				if(subfix == '_parc'){ //Adicionar informacion de titulacion y de pop
					do_add_titulacion( lyr_parcelas, "source_predio = \'\'" + op.lyr_parc + "\'\'")
					do_add_pop( lyr_parcelas, "source_predio = \'\'" + op.lyr_parc + "\'\'")
				}

				if(subfix == '_tit') //adicionando informacion de pop
					do_add_pop( lyr_parcelas, "source_parcela = \'\'coberturas.parcelas_tituladas\'\'")

				//adicionando informacion de ubicacion: departamento, provincia, municipio.
				do_add_ubicacion(lyr_parcelas, "nom_mun IS NULL")

				var arr = Array.isArray(res)?res:[res]
				var row_inserted = 0
				for (var i = 0, len = arr.length; i < len; i++) {
					row_inserted += (arr[i].command == 'INSERT' || arr[i].command == 'SELECT'? arr[i].rowCount :0)
				}

				resolve(row_inserted) //Completa la promesa para los procesos que estan esperando el resultado.
			})
		})
	}

	const do_search_predios = function(lyrs_predio){
		var lyrs = JSON.parse(JSON.stringify(lyrs_predio))
		var features_inters_cnt = 0
		return new Promise(function(resolve, reject) {
			var listPromise = []

			const search_lyr_predio = function(lyrs){
				if(!lyrs.length){
					return reject(new Error('search_lyrs_predio: No existen capas de predios configuradas en la variable "lyrs".') )
				}
				//--> ANALIZANDO CADA COBERTURAS DE PREDIOS/PARCELASlyrs
				var lyr_predio = lyrs.shift()
	    	//console.log(
				if(a!=='' && lyr_predio.lyr_parc){ //Si existen poligonos para localizar.
					var params = {
						"a": a,
						"condition_a": _condition,
						"b": lyr_predio.lyr_parc.source,
						"subfix": lyr_predio.subfix,
						"tolerance": lyr_predio.tolerance || _tolerance,
						"add_diff": true,
						"temp": false
					}
					var sql = 'SELECT sicob_overlap(\'' + JSON.stringify(params) + '\')'
					executeScalar(sql,function(err, res){
						if(err) return reject(err)

						if(parseInt(res.features_inters_cnt) > 0){ //--> Si se han localizado predios.
							console.log(params, res)
							listPromise.push(
							do_actualizar_referencia(op.lyr_in,params.a, res) //--> Se actualiza la referencia (si es necesario).
								.then(
									function(){
										listPromise.push(
											do_registrar_resultado(lyr_predio, res.lyr_over) //--> Se agregan los resultados a la capa resultado.
											.then(function(row_cnt){
												features_inters_cnt += row_cnt //console.log('Resultado registrado, se inseraron ' + row_cnt + 'registros')
											})
										)
									}
								)
							)

							if(parseInt(res.features_diff_cnt) > 0){
								a = res.lyr_over
								_condition = 'id_b IS NULL'
							}
						}

						if(parseInt(res.features_diff_cnt) == 0 || (!lyrs.length) ){
							res.features_inters_cnt = features_inters_cnt
							res.lyr_predio = lyr_predio
							res.params = params
							Promise.all(listPromise).then(resolve(res))
							return
						}
						search_lyr_predio(lyrs)
					})
				}
			} //-> final funcion search_lyrs_predio

			search_lyr_predio(lyrs)
		})
	}


	do_configurar_capa_parcelas.then(function(){
		do_search_predios(lyrs_predio).then(function(res){
			console.log('TERMINO!!!', res)

			if(res.features_diff_cnt > 0){ //finalizo con parcelas sin identificar predios
				do_actualizar_referencia(op.lyr_in,res.params.a, res) //--> Se actualiza la referencia (si es necesario).
					.then(
						function(){
								do_registrar_resultado(res.lyr_predio,res.lyr_over,'id_b IS NULL') //--> Se agregan los resultados a la capa resultado.
								.then(function(row_cnt){
									console.log('Parcelas sin predios ' + row_cnt + 'registros')
								})
						}
					)
			}

		})
	}).catch(function (err) {
    console.log(err);
  })

}

app.get('/test_obtener_predio', (req, res, next) => {
	//var op = {"lyr_in":"uploads.f20170704gcfebdac5d7c097","lyr_parc":"uploads.f20170705ecgdbfafbc4c20f"}
	var op = {"lyr_in":"uploads.f20170926adcgefb27cabb75"}
	var r = sicob_obtener_predio(op)
	return res.status(200).json({success: true, "resultado": r});
})
