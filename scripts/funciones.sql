-- Inicializacion --

DROP TABLE recorrido_final;
DROP TABLE aux;
DROP TABLE matches;


CREATE TABLE recorrido_final
(
  periodo TEXT,
  usuario INTEGER,
  fecha_hora_ret TIMESTAMP NOT NULL,
  est_origen INTEGER NOT NULL,
  est_destino INTEGER NOT NULL,
  fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev>=fecha_hora_ret),
  PRIMARY KEY(usuario,fecha_hora_ret)
);


CREATE TABLE aux
(
	periodo TEXT,
  id_usuario TEXT,
  fecha_hora_retiro TEXT,
  origen_estacion TEXT,
  nombre_origen TEXT,
  destino_estacion TEXT,
  nombre_destino TEXT,
  tiempo_uso TEXT,
  fecha_creacion TEXT
);

CREATE TABLE matches
(
  periodo TEXT,
  id_usuario TEXT,
  fecha_hora_retiro TEXT,
  origen_estacion TEXT,
  nombre_origen TEXT,
  destino_estacion TEXT,
  nombre_destino TEXT,
  tiempo_uso TEXT,
  fecha_creacion TEXT,
  PRIMARY KEY(id_usuario,fecha_hora_retiro,tiempo_uso)
);


\copy aux FROM test1.csv csv header delimiter ';'
-- \copy aux FROM recorridos-realizados-2016.csv csv header delimiter ';'

-- https://stackoverflow.com/questions/16195986/isnumeric-with-postgresql --
CREATE OR REPLACE FUNCTION isnumeric(text) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
    x = $1::NUMERIC;
    RETURN TRUE;
EXCEPTION WHEN others THEN
    RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

-- Migracion --

CREATE OR REPLACE FUNCTION migracion()
RETURNS VOID AS $$
DECLARE
  tuple RECORD;
  aRec RECORD;
  match RECORD;
  cursor1 CURSOR FOR 
    SELECT DISTINCT id_usuario, fecha_hora_retiro 
    FROM aux 
    WHERE id_usuario IS NOT NULL AND fecha_hora_retiro IS NOT NULL AND origen_estacion IS NOT NULL AND destino_estacion IS NOT NULL
      AND tiempo_uso IS NOT NULL AND isnumeric(replace(replace(replace(tiempo_uso, 'H ', ''), 'MIN ', ''), 'SEG', '')) 
      AND CAST(replace(replace(replace(tiempo_uso, 'H ', ''), 'MIN ', ''), 'SEG', '') AS INTEGER) >= 0
    GROUP BY id_usuario, fecha_hora_retiro
    HAVING count(id_usuario) > 1;
	cursor2 CURSOR FOR 
    SELECT * 
    FROM aux 
    WHERE id_usuario IS NOT NULL AND fecha_hora_retiro IS NOT NULL AND origen_estacion IS NOT NULL AND destino_estacion IS NOT NULL
      AND tiempo_uso IS NOT NULL AND isnumeric(replace(replace(replace(tiempo_uso, 'H ', ''), 'MIN ', ''), 'SEG', '')) 
      AND CAST(replace(replace(replace(tiempo_uso, 'H ', ''), 'MIN ', ''), 'SEG', '') AS INTEGER) >= 0;
BEGIN

  -------------------------------------
  -- usuario + fecha_hora_ret unicos --
  -------------------------------------

  OPEN cursor1;
  LOOP
    FETCH cursor1 INTO tuple;
    EXIT WHEN NOT FOUND;

    INSERT INTO matches (SELECT * FROM aux WHERE
      tuple.id_usuario = aux.id_usuario AND tuple.fecha_hora_retiro = aux.fecha_hora_retiro
      ORDER BY CAST(replace(replace(replace(aux.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL));

    FOR match IN SELECT * FROM matches LIMIT 1 
    LOOP
      DELETE FROM aux WHERE (match.periodo = aux.periodo AND match.origen_estacion = aux.origen_estacion AND
        match.nombre_origen = aux.nombre_origen AND match.destino_estacion = aux.destino_estacion AND
        match.nombre_destino = aux.nombre_destino AND match.tiempo_uso = aux.tiempo_uso AND match.fecha_creacion = aux.fecha_creacion);
    END LOOP;

    FOR match IN SELECT * FROM matches OFFSET 2 
    LOOP
      DELETE FROM aux WHERE (match.periodo = aux.periodo AND match.origen_estacion = aux.origen_estacion AND
        match.nombre_origen = aux.nombre_origen AND match.destino_estacion = aux.destino_estacion AND
        match.nombre_destino = aux.nombre_destino AND match.tiempo_uso = aux.tiempo_uso AND match.fecha_creacion = aux.fecha_creacion);
    END LOOP;

    DELETE FROM matches;
  END LOOP;
  CLOSE cursor1;

  DROP TABLE matches;

  -------------------------------------

  OPEN cursor2;
 	LOOP
 		FETCH cursor2 INTO aRec;
 		EXIT WHEN NOT FOUND;

 		PERFORM insertar_recorrido(aRec.periodo, CAST(aRec.id_usuario AS INTEGER), CAST(aRec.fecha_hora_retiro AS TIMESTAMP), CAST(aRec.origen_estacion AS INTEGER), CAST(aRec.destino_estacion AS INTEGER), CAST(aRec.fecha_hora_retiro AS TIMESTAMP) + CAST(replace(replace(replace(aRec.tiempo_uso, 'H ', 'H'), 'MIN ', 'M'), 'SEG', 'S') AS INTERVAL));
 	END LOOP;
 	CLOSE cursor2;

 	DROP TABLE aux;
 	PERFORM agregar_trigger();
  EXCEPTION
    WHEN OTHERS THEN
      raise notice '% %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION insertar_recorrido(periodo TEXT, usuario INTEGER, fecha_hora_ret TIMESTAMP, est_origen INTEGER, est_destino INTEGER, fecha_hora_dev TIMESTAMP)
RETURNS VOID AS $$
#variable_conflict use_column
DECLARE
  tuple RECORD;
  pfecha_hora_ret TIMESTAMP = fecha_hora_ret;
  pest_origen INTEGER = est_origen;
  pest_destino INTEGER = est_destino;
  pfecha_hora_dev TIMESTAMP = fecha_hora_dev;
BEGIN

  ---------------------------
  -- solapados encadenados --
  ---------------------------

  FOR tuple IN SELECT * FROM recorrido_final 
  LOOP
    IF usuario = tuple.usuario AND (fecha_hora_ret <= tuple.fecha_hora_dev) AND (fecha_hora_dev >= tuple.fecha_hora_ret) THEN
      CASE
        WHEN fecha_hora_ret <= tuple.fecha_hora_ret THEN
          -- tupla dada solapa menor a tupla de tabla --
          UPDATE recorrido_final SET fecha_hora_ret = pfecha_hora_ret, est_origen = pest_origen WHERE recorrido_final = tuple;
        ELSE
          -- tupla de tabla solapa menor a tupla dada --
          UPDATE recorrido_final SET est_destino = pest_destino, fecha_hora_dev = pfecha_hora_dev WHERE recorrido_final = tuple;
      END CASE;
    RETURN;
    END IF;
  END LOOP;

  ---------------------------

  INSERT INTO recorrido_final VALUES(periodo, usuario, fecha_hora_ret, est_origen, est_destino, fecha_hora_dev);

  EXCEPTION
	WHEN OTHERS THEN
		raise notice '% %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE PLPGSQL;


-- Trigger --

CREATE OR REPLACE FUNCTION agregar_trigger()
RETURNS VOID AS $$
BEGIN
	CREATE TRIGGER detecta_solapado BEFORE INSERT ON RECORRIDO_FINAL
	FOR EACH ROW
	EXECUTE PROCEDURE detecta_solapado();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION detecta_solapado()
RETURNS Trigger AS $$
DECLARE
 	countSolapados INT;
BEGIN
	SELECT count(*) INTO countSolapados
	FROM RECORRIDO_FINAL
	WHERE usuario = new.usuario AND (fecha_hora_ret <= new.fecha_hora_dev) AND (fecha_hora_dev >= new.fecha_hora_ret);

	IF (countSolapados > 0) THEN
		RAISE EXCEPTION 'INSERCION IMPOSIBLE POR SOLAPAMIENTO';
	END IF;

	RETURN new;
END;
$$ LANGUAGE plpgsql;
