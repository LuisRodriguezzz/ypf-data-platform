-- Unicidad del grano de fact_reservas. Va como test singular y no como `unique` de columna
-- porque la clave es compuesta y el modelo no publica una clave surrogate.
-- El test pasa cuando no devuelve filas.

select
    operador,
    cuenca,
    provincia,
    yacimiento,
    hoja,
    tipo_recurso,
    categoria,
    certeza,
    fluido,
    anio_corte,
    count(*) as filas
from {{ ref('fact_reservas') }}
group by all
having count(*) > 1
