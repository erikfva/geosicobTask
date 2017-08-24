const PATH_OGR2OGR = 'C:\\OSGeo4W64\\bin\\ogr2ogr.exe'
const PATH_SHP = 'C:\\wamp64\\www\\abt\\shapefiles\\'
const URL_SHP = 'http://localhost/abt/shapefiles/'
const SERVER_PORT = 3000

const express = require('express')
const app = express()
const shell = require('shelljs')
const pg = require('pg')
//Para compresion zip:
var fs = require('fs');
var archiver = require('archiver');

const config = {
  user: 'admderechos', //env var: PGUSER 
  database: 'geodatabase', //env var: PGDATABASE 
  //password: 'secret', //env var: PGPASSWORD 
  host: 'localhost', // Server hosting the postgres database 
  port: 5432, //env var: PGPORT 
  max: 10, // max number of clients in the pool 
  idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed 
}

	const pool = new pg.Pool(config)
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
  var idgeoproceso = parseInt(req.params.id);
  
  pool.connect(function(err, client, done) {
    if(err) {
      console.error('error fetching client from pool', err);
      return res.status(500).json({success: false, msg: err.message});
    }
    
    client.query('SELECT idusuario, proceso, entrada, salida FROM registro_derecho.geoprocesamiento WHERE idgeoproceso =' + parseInt(idgeoproceso) , function(err, result) {
      //call `done(err)` to release the client back to the pool (or destroy it if there is an error) 
      done(err);
   
      if(err) {
        return console.error('error running query', err);
      }
      console.log('Proceso: ' + result.rows[0].proceso);
      result.rows[0].entrada.idgeoproceso = idgeoproceso
      switch (result.rows[0].proceso) {
      	case 'sicob_build_shapefiles':
      		//Registrando inicio del proceso en el log.
      		sicob_log_geoproceso('{"idgeoproceso":"' + idgeoproceso + '", "exec_point":"inicio"}',function(err, res){
      			if(err){
      				console.log('sicob_build_shapefiles(' + idgeoproceso + '): Error al registrar el log inicial!!! ' + err)
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

app.listen(SERVER_PORT, function () {
  console.log('Servicios de geoprocesamiento disponibles en el puerto ' + SERVER_PORT + '!')
})

function executeSQL(sql,cb){
  // to run a query we can acquire a client from the pool, 
  // run a query on the client, and then return the client to the pool 

  pool.query(sql, (err, res) => {
  	if (err) {
    	if(cb) cb(err, null)
    	return;
  	}
  	//console.log('ejecutado ' +  sql)
   	if(cb) cb(null, res.rows)
	})
}

function batchSQL(sqlList,cb,finish){
	
	var sqlPending = sqlList.length
	var execNext = function(){
		var sql = sqlList.shift()
		console.log('ejecutando...' + sql)
		executeSQL(sql,function(err,rows){
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
	shell.mkdir('-p', PATH_SHP + timesid)
	
	data.lyr_list.forEach(function (lyr) {
		console.log(lyr) 
		var cmd = PATH_OGR2OGR + ' -f "ESRI Shapefile" ' + PATH_SHP + timesid + '/' +(lyr.fname || lyr.lyr) + '.shp PG:"host=' + config.host + ' user=' + config.user + ' dbname=' + config.database + '" -geomfield the_geom -overwrite --config PG_USE_COPY YES "' + lyr.lyr + '"'
		cmds.push(cmd)         
	})
	var files_cnt = data.lyr_list.length
	var error = new Error('')
	
	batch(
		cmds,
		function(cmd, code, stdout, stderr){
			console.log('(sicob_build_shapefiles) ejecutó...',cmd)
			if(code){
				files_cnt--
				console.log('Error (sicob_build_shapefiles): ' + stderr)
				error.message = error.message==''? 'Error (sicob_build_shapefiles/zip):' + stderr : error.message + ', ' + stderr
			}
		},
		function(){
			console.log('(sicob_build_shapefiles)...terminó los shp.')
			zip({
				globpatt : '{*.shp,*.dbf,*.shx,*.prj}', //Patron usado por node-glob para generalizar nombres de archivos.
				ftarget : PATH_SHP + fzipname + '.zip', //Nombre del archivo .zip generado, incluyendo la dirección de carpeta.
				workdir : PATH_SHP + timesid + '/', //(opcional) Directorio donde se encuentran los archivos a comprimir.
				cb : function(err){ //(opcional) Función de callback que se ejecuta al finalizar el trabajo.
					var result = {link : URL_SHP + fzipname + '.zip', shp_cnt : files_cnt, success : 1}
					execute('rmdir /S /Q "' + PATH_SHP + timesid + '"');
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
			//console.log('Se ejecutó...',sql)
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

/*** FUNCION PARA COMPRIMIR ARCHIVOS EN FORMATO ZIP ***/
/*	Variables de op:		
				globpatt : '{*.shp,*.dbf,*.shx,*.prj}', //Patron usado por node-glob para generalizar nombres de archivos.
				ftarget : PATH_SHP + fzipname + '.zip', //Nombre del archivo .zip generado, incluyendo la dirección de carpeta.
				workdir : PATH_SHP + timesid + '/', //(opcional) Directorio donde se encuentran los archivos a comprimir.
				cb : function(err){...} //(opcional) Función de callback que se ejecuta al finalizar el trabajo.
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