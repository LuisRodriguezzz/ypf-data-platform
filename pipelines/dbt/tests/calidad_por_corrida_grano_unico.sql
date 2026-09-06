-- Unicidad del grano de calidad_por_corrida (contrato + fecha). Va como test singular y no
-- como `unique` de columna porque la clave es compuesta.
-- El test pasa cuando no devuelve filas.
--
-- Se apaga fuera de local por el mismo motivo que el modelo que prueba (ver `dbt_project.yml`):
-- un test que referencia un modelo deshabilitado rompe la compilación.
{{ config(enabled = target.name == 'local') }}

select
    contrato,
    fecha,
    count(*) as filas
from {{ ref('calidad_por_corrida') }}
group by
    contrato,
    fecha
having count(*) > 1
