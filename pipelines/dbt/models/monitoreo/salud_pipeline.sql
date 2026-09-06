-- Salud del pipeline: una fila por tabla publicada de silver y gold, con cuánto tiene, cuándo
-- se cargó por última vez y qué dijo el control de calidad de su contrato. Es la tabla que se
-- mira primero cuando alguien pregunta "¿está andando esto?".
--
-- **El inventario es un `union all` escrito a mano y no un bucle sobre el catálogo.** Iceberg
-- expone las tablas por su API REST, no por SQL, así que armarlo dinámicamente pediría una
-- macro que consulte el catálogo en tiempo de compilación: más piezas para leer y un modelo
-- cuyo alcance cambia solo. Escritas, las 14 filas se leen de corrido y agregar una tabla son
-- tres líneas.
--
-- **De dónde sale `ultima_carga`.** En silver, de la columna que `silver_load.py` escribe en
-- cada fila. En gold no hay columna equivalente —los modelos de dbt no la llevan— así que sale
-- del último snapshot de la tabla Iceberg, que es el dato exacto de cuándo se escribió.

with inventario as (

    -- Silver. `_silver_loaded_at` lo pone el job en cada fila que publica (ADR 0005).
    select
        'silver' as capa,
        'produccion_pozo' as tabla,
        'produccion_pozo' as contrato,
        count(*) as filas,
        max(_silver_loaded_at) as ultima_carga
    from {{ source('silver', 'produccion_pozo') }}

    union all
    select 'silver', 'pozo_primera_produccion', 'pozo_primera_produccion',
        count(*), max(_silver_loaded_at)
    from {{ source('silver', 'pozo_primera_produccion') }}

    union all
    select 'silver', 'fractura', 'fractura', count(*), max(_silver_loaded_at)
    from {{ source('silver', 'fractura') }}

    union all
    select 'silver', 'reservas', 'reservas', count(*), max(_silver_loaded_at)
    from {{ source('silver', 'reservas') }}

    -- La escribe el job de streaming y no un contrato: no tiene fila en `dq_runs`.
    union all
    select 'silver', 'telemetria_pozo_1min', cast(null as string), count(*), max(_ingested_at)
    from {{ source('silver', 'telemetria_pozo_1min') }}

    -- Gold. La fecha de carga es la del último commit de la tabla Iceberg.
    union all
    select 'gold', 'dim_empresa', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('dim_empresa') }}.snapshots)
    from {{ ref('dim_empresa') }}

    union all
    select 'gold', 'dim_fecha', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('dim_fecha') }}.snapshots)
    from {{ ref('dim_fecha') }}

    union all
    select 'gold', 'dim_pozo', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('dim_pozo') }}.snapshots)
    from {{ ref('dim_pozo') }}

    union all
    select 'gold', 'dim_yacimiento', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('dim_yacimiento') }}.snapshots)
    from {{ ref('dim_yacimiento') }}

    union all
    select 'gold', 'fact_fractura', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('fact_fractura') }}.snapshots)
    from {{ ref('fact_fractura') }}

    union all
    select 'gold', 'fact_produccion_mensual', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('fact_produccion_mensual') }}.snapshots)
    from {{ ref('fact_produccion_mensual') }}

    union all
    select 'gold', 'fact_reservas', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('fact_reservas') }}.snapshots)
    from {{ ref('fact_reservas') }}

    union all
    select 'gold', 'mart_pozo_completacion_produccion', cast(null as string), count(*),
        (select max(committed_at) from {{ ref('mart_pozo_completacion_produccion') }}.snapshots)
    from {{ ref('mart_pozo_completacion_produccion') }}

    -- La escribe `pipelines/ml/predecir.py` con pyiceberg, no dbt: acá es un source y la
    -- fecha de la corrida viaja en una columna propia (ADR 0012).
    union all
    select 'gold', 'prediccion_produccion_12m', cast(null as string), count(*), max(predicho_en)
    from {{ source('gold', 'prediccion_produccion_12m') }}
),

-- Una fila por contrato y corrida. Una corrida toca varios recursos y alcanza con que uno
-- falle el check duro para que la corrida entera cuente como fallada.
corridas as (
    select
        contract as contrato,
        run_at,
        sum(rows_rejected) as rechazadas,
        case
            when max(case when status = 'failed' then 1 else 0 end) = 1 then 'failed'
            else 'ok'
        end as estado
    from {{ source('silver', 'dq_runs') }}
    group by contract, run_at
),

calidad as (
    select
        contrato,
        sum(rechazadas) as filas_rechazadas_historicas,
        max(run_at) as ultima_corrida_calidad,
        -- `run_at` es único dentro del grupo (arriba se agrupó por él), así que no hay empate.
        max_by(estado, run_at) as estado_ultima_corrida
    from corridas
    group by contrato
),

-- Cuánto hay hoy en cuarentena. `pozo_primera_produccion` no aparece porque su contrato nunca
-- rechazó una fila y la tabla `_rejects` no llega a crearse: ahí la columna queda nula.
cuarentena as (
    select 'produccion_pozo' as contrato, count(*) as filas_en_cuarentena
    from {{ source('silver', 'produccion_pozo_rejects') }}
    union all
    select 'fractura', count(*) from {{ source('silver', 'fractura_rejects') }}
    union all
    select 'reservas', count(*) from {{ source('silver', 'reservas_rejects') }}
)

select
    i.capa,
    i.tabla,
    i.contrato,
    i.filas,
    i.ultima_carga,
    {{ dias_entre('cast(i.ultima_carga as date)', 'current_date()') }} as dias_desde_la_carga,
    c.filas_rechazadas_historicas,
    q.filas_en_cuarentena,
    c.ultima_corrida_calidad,
    c.estado_ultima_corrida
from inventario i
left join calidad c on i.contrato = c.contrato
left join cuarentena q on i.contrato = q.contrato
order by i.capa, i.tabla
