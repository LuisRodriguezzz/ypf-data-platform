-- `dq_runs` resumida por contrato y día: cuántas filas leyó silver, cuántas publicó, cuántas
-- mandó a cuarentena y qué proporción representa. Es la serie con la que se ve si la calidad
-- de una fuente se está degradando, en vez de mirar una corrida suelta.
--
-- El grano es contrato + día y no contrato + corrida porque una carga mensual dispara varias
-- corridas seguidas (una por recurso pendiente) y lo que interesa comparar es un día contra
-- otro. El detalle por recurso sigue entero en `silver.dq_runs`.

select
    contract as contrato,
    cast(run_at as date) as fecha,
    count(*) as recursos,
    sum(rows_in) as filas_leidas,
    sum(rows_out) as filas_publicadas,
    sum(rows_rejected) as filas_rechazadas,
    -- Tasa sobre lo leído. `nullif` evita dividir por cero en una corrida que no leyó nada.
    round(sum(rows_rejected) / nullif(sum(rows_in), 0), 6) as tasa_de_rechazo,
    sum(case when status = 'failed' then 1 else 0 end) as recursos_con_falla_dura
from {{ source('silver', 'dq_runs') }}
group by contract, cast(run_at as date)
order by fecha desc, contrato
