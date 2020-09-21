const XLSX = require('xlsx');
var mysql = require('mysql2/promise');

var option = {
	host: 'localhost',
	user: 'sofi',
	password:'SOFI2020#Sanofi',
	database: 'sofi_production'
};
let rows, r;
let cont = 0, cant = 0;
let index0, index1;
let date;

leer_e_insertar();

async function leer_e_insertar() {
	try{
		console.log('///////////////////      ESTABLECIENDO CONEXION CON LA BD...     ////////////////////');
		const connection = await mysql.createConnection(option);
		console.log('\n///////////////////      ¡CONEXION ESTABLECIDA!     ////////////////////');
		console.log('\n///////////////////      PROCESANDO ARCHIVO...     ////////////////////\n');
		const file = XLSX.readFile('ESPECIALIDADES.xlsx');
		const fileSheet = file.SheetNames
		// Hoja 1
		const sheet1 = fileSheet[0]
		const dataSheet1 = XLSX.utils.sheet_to_json(file.Sheets[sheet1])
		// Hoja 2
		const sheet2 = fileSheet[1]
		const dataSheet2 = XLSX.utils.sheet_to_json(file.Sheets[sheet2])

		// DEFINICION DE LAS ANTIGUAS Y NUEVAS ESPECIALIDADES
		let antes_desps = { antes: [], desps: [], hcp: [] }
		for (let i = 0; i < dataSheet1.length; i++) {
			antes_desps.antes.push((dataSheet1[i]['ORIGEN']).trim());
			antes_desps.desps.push((dataSheet1[i]['DESTINO']).trim());
			if(dataSheet1[i]['HCP']){antes_desps.hcp.push((dataSheet1[i]['HCP']).trimEnd());}
		}

		// NUEVAS ESPECIALIDADES
		let especialidades = { Esp: [], Cod: []}
		for (let i = 0; i < dataSheet2.length; i++) {
			especialidades.Esp.push(dataSheet2[i]['ESPECIALIDAD UNIFICADA']);
			especialidades.Cod.push(dataSheet2[i]['CODIGO']);
		}

		console.log('\n///////////////////      ¡ARCHIVO PROCESADO!     ////////////////////\n');
		console.log('\n\n\n-----------------------------         -------------------------\n\n\n');

		// INSERTAR LAS NUEVAS ESPECIALIDADES
		console.log('\n///////////////////      INSERTANDO LAS NUEVAS ESPECIALIDADES...     ////////////////////\n');
		date = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');
		for (let i = 0; i < especialidades.Esp.length; i++) {
			r = await connection.query(
				`INSERT INTO specialities (name, description, specialityId, created_at, updated_at)
				VALUES ('${especialidades.Esp[i]}', 'Descripción', '${especialidades.Cod[i]}', '${date}', '${date}');`);
			cont += r[0].affectedRows;
		}
		console.log('Nº DE ESPECIALIDADES INSERTADAS: ' + cont);

		// BUSCAR EL ID DE LA ESPECIALIDAD 'OTRA ESPECIALIDAD CLÍNICA'
		[rows] = await connection.query(
			`SELECT id
			FROM specialities
			WHERE name = 'OTRA ESPECIALIDAD CLÍNICA';`
		);

		// REEMPLAZAR EL ID DE LAS ESPECIALIDADES QUE ESTEN EN NULL POR EL ID ANTERIOR
		console.log('\n/////  REMPLAZANDO EL ID DE LAS (SUB)ESPECIALIDADES QUE ESTEN EN NULL POR EL ID \'OTRA ESPECIALIDAD CLÍNICA\'  /////');
		r = await connection.query(
			`UPDATE users SET
			speciality_id = ${rows[0].id}
			WHERE speciality_id IS NULL;`
		);
		console.log('Nº ESPECIALIDADES ACTUALIZADAS:', r[0].info);

		// REEMPLAZAR EL ID DE LAS SUBESPECIALIDADES QUE ESTEN EN NULL POR EL ID ANTERIOR
		r = await connection.query(
			`UPDATE users u SET
			u.sub_speciality_id = ${rows[0].id}
			WHERE u.sub_speciality_id IS NULL;`
		);
		console.log('Nº SUBESPECIALIDADES ACTUALIZADAS:', r[0].info);

		// BUSCAR TODAS LAS ESPECIALIDADES Y SUBESPECIALIDADES DE LOS USUARIOS
		[rows] = await connection.query(
			`SELECT u.id AS usuario, s.name AS especialidad,
				(SELECT sp.name
				FROM specialities sp
				WHERE u.sub_speciality_id = sp.id) AS sub_especialidad
			FROM users u, specialities s
			WHERE (u.speciality_id = s.id);`
		);

		// REEMPLAZAR EL NOMBRE DE LAS ANTIGUAS ESPECIALIDADES/SUBESPECIALIDADES POR LAS NUEVAS
		for(let i=0; i<rows.length; i++){
			index0 = antes_desps.antes.findIndex(element => element === rows[i].especialidad);
			index1 = antes_desps.antes.findIndex(element => element === rows[i].sub_especialidad);
			if(index0 != -1) rows[i].especialidad = antes_desps.desps[index0]
			if(index1 != -1) rows[i].sub_especialidad = antes_desps.desps[index1]
		}

		// ACTUALIZAR EL ID DE LAS ANTIGUAS ESPECIALIDADES/SUBESPECIALIDADES POR LAS NUEVAS EN BD
		console.log('\n/////  ACTUALIZANDO EL ID DE LAS ANTIGUAS (SUB)ESPECIALIDADES POR LAS NUEVAS DE LOS USUARIOS  /////');
		cont = 0;
		for(let i=0; i<rows.length; i++){
			r = await connection.query(
				`UPDATE users u SET
					u.speciality_id =
						(SELECT s.id FROM specialities s WHERE s.name = '${rows[i].especialidad}' AND s.id > 112),
					u.sub_speciality_id =
						(SELECT s.id FROM specialities s WHERE s.name = '${rows[i].sub_especialidad}' AND s.id > 112)
				WHERE u.id = ${rows[i].usuario};`
			);
			cont += r[0].affectedRows;
		 	if(r[0].length == 0) console.log(`${rows[i].especialidad}`);
		}
		console.log('Nº DE USUARIOS ACTUALIZADOS: ' + cont);

		console.log('\n/////////////////////////////////////////////////////////////////////');
		console.log('////////////                                              ///////////');
		console.log('////////      ¡ESPECIALIDADES DE USUARIOS ACTUALIZADOS!      ////////');
		console.log('///////////                                               ///////////');
		console.log('/////////////////////////////////////////////////////////////////////');
		console.log('\n\n\n-----------------------------         -------------------------\n\n\n');

		// BUSCAR TODAS LAS ESPECIALIDADES RELACIONADAS DE LOS CASOS CLINICOS
		[rows] = await connection.query(
			`SELECT ccs.speciality_id AS id, s.name AS nombre, ccs.clinical_case_id AS cc
			FROM specialities s, clinical_case_specialities ccs
			WHERE (ccs.speciality_id = s.id) ORDER BY cc ASC;`
		);

		// REEMPLAZAR EL NOMBRE DE LAS ANTIGUAS ESPECIALIDADES/SUBESPECIALIDADES POR LAS NUEVAS
		for(let i=0; i<rows.length; i++){
			index0 = antes_desps.antes.findIndex(element => element === rows[i].nombre);
			if(index0 != -1) rows[i].nombre = antes_desps.desps[index0];
		}

		// ACTUALIZAR EL ID DE LAS ANTIGUAS ESPECIALIDADES/SUBESPECIALIDADES POR LAS NUEVAS EN BD
		console.log('\n/////  ACTUALIZANDO EL ID DE LAS ANTIGUAS (SUB)ESPECIALIDADES POR LAS NUEVAS DE LOS CC  /////');
		date = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');
		cont = 0;
		for(let i=0; i<rows.length; i++){
			try{
				r = await connection.query(
					`UPDATE clinical_case_specialities ccs SET ccs.speciality_id =
					(SELECT s.id FROM specialities s WHERE s.name = '${rows[i].nombre}' AND s.id > 112),
					updated_at = '${date}'
					WHERE ccs.clinical_case_id = ${rows[i].cc} AND ccs.speciality_id = ${rows[i].id};`
				);
				cont += r[0].affectedRows;
			}catch(e){ cant++; }
		}
		console.log('Nº DE CASOS CLINICOS ACTUALIZADOS: ' + cont);
		console.log('Nº DE ERRORES: ' + cant);

		// ELIMINAR LOS CASOS CLINICOS CON ESPECIALIDADES ANTIGUAS
		console.log('\n//////////////      ELIMINANDO LOS CASOS CLINICOS CON ESPECIALIDADES ANTIGUAS...     ///////////////');
		r = await connection.query( `DELETE FROM clinical_case_specialities WHERE speciality_id < '113';` );
		console.log('Nº FILAS ELIMINADAS', r[0].affectedRows);

		console.log('\n/////////////////////////////////////////////////////////////////////');
		console.log('/////////                                                   /////////');
		console.log('///////   ¡ESPECIALIDADES DE CASOS CLINICOS ACTUALIZADOS!     ///////');
		console.log('/////////                                                   /////////');
		console.log('/////////////////////////////////////////////////////////////////////');
		console.log('\n\n\n-----------------------------         -------------------------\n\n\n');

		// BUSCAR TODAS LAS ESPECIALIDADES DE INTERES DE LOS USUARIOS
		[rows] = await connection.query(
			`SELECT su.speciality_id AS id, s.name AS nombre, su.user_id AS user
			FROM specialities s, speciality_users su
			WHERE su.speciality_id = s.id`
		);

		// REEMPLAZAR EL NOMBRE DE LAS ANTIGUAS ESPECIALIDADES/SUBESPECIALIDADES POR LAS NUEVAS
		for(let i=0; i<rows.length; i++){
			index0 = antes_desps.antes.findIndex(element => element === rows[i].nombre);
			if(index0 != -1) rows[i].nombre = antes_desps.desps[index0]
		}

		// ACTUALIZAR EL ID DE LAS ANTIGUAS ESPECIALIDADES/SUBESPECIALIDADES POR LAS NUEVAS EN BD
		console.log('\n/////  ACTUALIZANDO EL ID DE LAS ANTIGUAS (SUB)ESPECIALIDADES DE INTERES  /////');
		date = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '');
		cont = 0; cant = 0;
		for(let i=0; i<rows.length; i++){
			try{
				r = await connection.query(
					`UPDATE speciality_users su SET su.speciality_id =
					(SELECT s.id FROM specialities s WHERE s.name = '${rows[i].nombre}' AND s.id > 112),
					updated_at = '${date}'
					WHERE su.speciality_id = ${rows[i].id} AND su.user_id = ${rows[i].user}`
				);
				cont += r[0].affectedRows;
			}catch(e){ cant++; }
		}
		console.log('Nº DE ESPECIALIDADS DE INTERES ACTUALIZADOS: ' + cont);
		console.log('Nº DE ERRORES: ' + cant);

		// ELIMINAR LAS ESPECIALIDADES DE INTERES ANTIGUAS
		console.log('\n//////////////      ELIMINANDO LAS ESPECIALIDADES DE INTERES ANTIGUAS...     ///////////////');
		r = await connection.query( `DELETE FROM speciality_users WHERE speciality_id < '113';` );
		console.log('Nº FILAS ELIMINADAS', r[0].affectedRows);

		console.log('\n\n\n-----------------------------         -------------------------\n\n\n');
		console.log('----------------------------------------------------------------------');
		console.log('----------------------------------------------------------------------');
		console.log('-----------------------       REVISANDO        -----------------------');
		console.log('----------------------------------------------------------------------');
		console.log('----------------------------------------------------------------------');

		// CONSULTAR LA CANTIDAD DE USUARIOS A LOS QUE NO SE ACTUALIZARON SUS ESPECIALIDADES
		console.log('\n///////////////     CONSULTANDO LOS USUARIOS CON ESPECIALIDADES ANTIGUAS     ///////////////');
		[rows] = await connection.query(`SELECT COUNT(*) AS cant FROM sofi_production.users WHERE speciality_id < 113;`);
		console.log('CANTIDAD DE ESPECIALIDADS NO ACTUALIZADAS: ' + rows[0].cant);

		// CONSULTAR LOS USUARIOS A LOS QUE NO SE ACTUALIZARON SUS SUBESPECIALIDADES
		console.log('\n///////////////     CONSULTANDO LOS USUARIOS CON SUBESPECIALIDADES ANTIGUAS     ///////////////');
		[rows] = await connection.query(`SELECT COUNT(*) AS cant FROM sofi_production.users WHERE sub_speciality_id < 113;`);
		console.log('CANTIDAD DE SUBESPECIALIDADS NO ACTUALIZADAS: ' + rows[0].cant);

		// CONSULTAR LOS USUARIOS A LOS QUE LAS ESPECIALIDADES QUEDARON EN NULL
		console.log('\n///////////////     CONSULTANDO LOS USUARIOS A LOS QUE LAS ESPECIALIDADES QUEDARON EN NULL     ///////////////');
		[rows] = await connection.query(`SELECT COUNT(*) AS cant FROM sofi_production.users WHERE speciality_id IS NULL;`);
		console.log('Nº DE ESPECIALIDADS EN NULL: ' + rows[0].cant);

		// CONSULTAR LOS USUARIOS A LOS QUE LAS SUBESPECIALIDADES QUEDARON EN NULL
		console.log('\n///////////////     CONSULTANDO LOS USUARIOS A LOS QUE LAS SUBESPECIALIDADES QUEDARON EN NULL     ///////////////');
		[rows] = await connection.query(`SELECT COUNT(*) AS cant FROM sofi_production.users WHERE sub_speciality_id IS NULL;`);
		console.log('Nº DE SUBESPECIALIDADS EN NULL: ' + rows[0].cant);

		// CONTAR LA CANTIDAD DE SUB/ESPECIALIDADES DE INTERES DE USUARIOS QUE NO SE ACTUALIZARON
		console.log('\n///////////////     CONSULTANDO LOS USUARIOS CON ESPECIALIDADES ANTIGUAS     ///////////////');
		[rows] = await connection.query(`SELECT COUNT(*) AS cant FROM sofi_production.speciality_users WHERE speciality_id < 113;`);
		console.log('CANTIDAD DE ESPECIALIDADS DE INTERES NO ACTUALIZADAS: ' + rows[0].cant);

		// ELIMNAR LAS ESPECIALIDADES ANTIGUAS
		console.log('\n///////////     ELIMINANDO LAS ESPECIALIDADES ANTIGUAS     ////////////////');
		try{
			r = await connection.query(`DELETE FROM sofi_production.specialities WHERE id < 113;`);
			console.log('Nº FILAS ELIMINADAS', r[0].affectedRows);
		}catch(e){ console.log('ERROR', e) }

		console.log('\n\n\n-----------------------------         -------------------------\n\n\n');
		console.log('\n////////////////////////////////////////////////////////////////////////////');
		console.log('////////////////                                          ///////////////////');
		console.log('\n///////////      ¡ESPECIALIDADES ANTIGUAS ELIMINANDAS!      ///////////////');
		console.log('////////////////                                          ///////////////////');
		console.log('\n///////////////////////////////////////////////////////////////////////////');

		console.log('\n/////////////////////////////////////////////////////////////////////');
		console.log('/////////////////////                            ////////////////////');
		console.log('///////////////////      ¡SCRIPT FINALIZADO!       //////////////////');
		console.log('////////////////////                             ////////////////////');
		console.log('/////////////////////////////////////////////////////////////////////');

		connection.end();
		process.exit(0);
	}catch(e){
		console.log('ERROR', e);
	}
}