-- SOLO SELECT. No ejecutar DDL sobre las siete tablas actuales.
-- Athena escribe resultados en athena-results/: estas consultas NO se ejecutaron en la auditoría read-only.
-- CT-10: cotejar resultados con las mismas selecciones/filtros del dashboard.
-- 1. OTP por ruta, aeropuerto, franja y mes: sumar numeradores, nunca promediar porcentajes.
SELECT anio,mes,ruta,aeropuerto_origen,aeropuerto_destino,franja_horaria,
 sum(total_vuelos_puntuales) puntuales,sum(total_vuelos_operados) operados,
 sum(total_vuelos_desviados) desviados,
 100.0*sum(total_vuelos_puntuales)/nullif(sum(total_vuelos_operados),0) otp_pct
FROM vueladata_gold_db.puntualidad_rutas GROUP BY 1,2,3,4,5,6;
-- 2. Causas y participación imputable.
SELECT anio,mes,codigo_causa,es_imputable,sum(total_minutos_retraso) minutos,
 100.0*sum(total_minutos_retraso)/nullif(sum(sum(total_minutos_retraso)) OVER(PARTITION BY anio,mes),0) participacion_pct
FROM vueladata_gold_db.causas_retraso GROUP BY 1,2,3,4;
-- 3. Costo por causa/ruta/mes (COP).
SELECT anio,mes,codigo_causa,ruta,sum(monto_total_compensado_cop) monto_cop,
 sum(total_vuelos_con_compensacion) vuelos
FROM vueladata_gold_db.costos_compensacion GROUP BY 1,2,3,4;
-- 4. Conexiones perdidas en hubs.
SELECT anio,mes,aeropuerto_hub,sum(total_conexiones_perdidas) perdidas,
 sum(total_conexiones_programadas) programadas
FROM vueladata_gold_db.conexiones_hubs GROUP BY 1,2,3;
-- 5. Propagación por matrícula, ponderada por vuelos.
SELECT anio,mes,matricula_aeronave,
 sum(retraso_propagado_promedio_min*total_vuelos_operados)/nullif(sum(total_vuelos_operados),0) promedio_min
FROM vueladata_gold_db.propagacion_flota GROUP BY 1,2,3;
-- 6. Impacto climático por aeropuerto.
SELECT anio,mes,aeropuerto_origen,sum(total_vuelos_afectados_clima) afectados,
 sum(total_minutos_retraso_clima) minutos
FROM vueladata_gold_db.impacto_clima GROUP BY 1,2,3 ORDER BY afectados DESC;
-- Estas consultas validan los cubos existentes; no prueban reglas sobre fuentes crudas.
-- OTP mensual por aerolínea, conservando el denominador oficial del cubo.
SELECT anio,mes,codigo_aerolinea,
 sum(total_vuelos_desviados) desviados,
 100.0*sum(total_vuelos_puntuales)/nullif(sum(total_vuelos_operados),0) otp_pct
FROM vueladata_gold_db.puntualidad_aerolineas GROUP BY 1,2,3;
-- Tras registrar las tablas nuevas: RN-10 y unicidad de hechos.
SELECT count(*) violaciones_otp FROM vueladata_gold_db.fact_vuelos WHERE es_desviado=1 AND es_puntual_otp<>0;
SELECT id_vuelo_gold,count(*) copias FROM vueladata_gold_db.fact_vuelos GROUP BY 1 HAVING count(*)>1;
SELECT count(*) huerfanos FROM vueladata_gold_db.fact_vuelos f LEFT JOIN vueladata_gold_db.dim_ruta d
 ON f.id_ruta_gold=d.id_ruta_gold WHERE d.id_ruta_gold IS NULL;
-- CA-01: solo vuelos operados, con retraso conocido. Resultado esperado: cero.
SELECT count(*) violaciones_ca01 FROM vueladata_gold_db.fact_vuelos
WHERE es_cancelado=0 AND es_desviado=0 AND retraso_llegada_min IS NOT NULL
AND es_puntual_otp IS DISTINCT FROM CASE WHEN retraso_llegada_min<=15 THEN 1 ELSE 0 END;
-- CA-02/03: escala económica en vuelos no cancelados con imputabilidad conocida.
-- La cancelación <72h no es verificable aquí: el hecho no expone el aviso previo.
SELECT count(*) violaciones_escala FROM vueladata_gold_db.fact_compensaciones c
JOIN vueladata_gold_db.fact_vuelos v ON c.id_vuelo_gold=v.id_vuelo_gold
WHERE v.es_cancelado=0 AND v.es_imputable IS NOT NULL AND v.retraso_llegada_min IS NOT NULL
AND c.porcentaje_compensacion IS DISTINCT FROM
 CASE WHEN NOT v.es_imputable THEN 0.0 WHEN v.retraso_llegada_min>360 THEN 1.0
 WHEN v.retraso_llegada_min>=240 THEN 0.5 WHEN v.retraso_llegada_min>=120 THEN 0.25 ELSE 0.0 END;
-- Controles de cobertura: NULL indica dato desconocido, no ausencia del fenómeno.
SELECT anio,mes,count_if(requiere_auditoria) vuelos_auditar,
 count_if(requiere_auditoria IS NULL) sin_dato_clima
FROM vueladata_gold_db.fact_vuelos GROUP BY 1,2;
SELECT anio,mes,aeropuerto_hub,count_if(es_conexion_en_riesgo) en_riesgo,
 count_if(es_conexion_perdida=1) perdidas,
 count_if(es_conexion_en_riesgo IS NULL) sin_dato_escala
FROM vueladata_gold_db.fact_conexiones GROUP BY 1,2,3;
-- CA-04/05 completos requieren Silver: visibilidad/viento, hora real del evento,
-- carácter nacional/internacional y secuencia de vuelos no están todos en Gold.
-- No inferir cumplimiento de esos criterios únicamente a partir de estos agregados.
