CREATE OR REPLACE FUNCTION detecta_solapado() RETURNS Trigger AS $$
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

CREATE TRIGGER detecta_solapado BEFORE INSERT ON RECORRIDO_FINAL
FOR EACH ROW
EXECUTE PROCEDURE detecta_solapado();