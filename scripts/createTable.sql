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
