{#
  Las dos claves surrogate que se calculan en más de un modelo. Están acá y no repetidas en
  cada SQL porque si dim_empresa y una fact normalizaran el nombre distinto, la fact quedaría
  apuntando a una empresa que no existe. El test `relationships` lo detectaría, pero es más
  barato tener una sola definición.

  El dialecto no vive acá: `md5` y el literal de la expresión regular los resuelve
  `macros/dialecto.sql`, que es el único archivo del proyecto que sabe en qué motor corre.
#}

{% macro nombre_empresa(columna) -%}
    regexp_replace(upper(trim({{ columna }})), {{ patron_espacios() }}, ' ')
{%- endmacro %}


{% macro clave_empresa(columna) -%}
    {{ md5(nombre_empresa(columna)) }}
{%- endmacro %}
