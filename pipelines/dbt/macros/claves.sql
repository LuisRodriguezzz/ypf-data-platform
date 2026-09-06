{#
  Las dos claves surrogate que se calculan en más de un modelo. Están acá y no repetidas en
  cada SQL porque si dim_empresa y una fact normalizaran el nombre distinto, la fact quedaría
  apuntando a una empresa que no existe. El test `relationships` lo detectaría, pero es más
  barato tener una sola definición.

  Es también el único lugar con dialecto: Athena escribe el hash como
  lower(to_hex(md5(to_utf8(x)))). Cuando entre el destino aws, la diferencia se resuelve acá
  y ningún modelo cambia (ADR 0002).
#}

{% macro nombre_empresa(columna) -%}
    regexp_replace(upper(trim({{ columna }})), '\\s+', ' ')
{%- endmacro %}


{% macro clave_empresa(columna) -%}
    md5({{ nombre_empresa(columna) }})
{%- endmacro %}
