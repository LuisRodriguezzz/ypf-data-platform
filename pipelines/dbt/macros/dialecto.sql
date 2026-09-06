{#
  Lo poco que Spark y Athena escriben distinto. Athena es Trino: comparte casi todo el
  dialecto con Spark SQL, y las media docena de funciones que no comparte se resuelven acá
  con `adapter.dispatch` para que los modelos tengan una sola versión del SQL (ADR 0010).

  Cada macro se llama como la función de Spark, así el modelo se sigue leyendo igual. Las
  excepciones son `fin_de_mes`, `dias_entre` y `meses_entre`: dbt-core ya publica macros
  `last_day` y `datediff` propias, y redefinirlas en el proyecto se las cambiaría también a
  dbt. `default__` es la implementación de Spark porque es la de local, el destino de todos
  los días.
#}


{% macro md5(expresion) -%}
    {{ return(adapter.dispatch('md5', 'gold')(expresion)) }}
{%- endmacro %}

{% macro default__md5(expresion) -%}
    md5({{ expresion }})
{%- endmacro %}

{% macro athena__md5(expresion) -%}
    {# En Trino md5 toma y devuelve varbinary. El cast es porque no toda clave es texto:
       `idareayacimiento` es un código y en Spark el casteo era implícito. #}
    lower(to_hex(md5(to_utf8(cast({{ expresion }} as varchar)))))
{%- endmacro %}


{% macro make_date(anio, mes, dia) -%}
    {{ return(adapter.dispatch('make_date', 'gold')(anio, mes, dia)) }}
{%- endmacro %}

{% macro default__make_date(anio, mes, dia) -%}
    make_date({{ anio }}, {{ mes }}, {{ dia }})
{%- endmacro %}

{% macro athena__make_date(anio, mes, dia) -%}
    {# Trino no arma fechas desde tres enteros: se escribe el ISO y se parsea. #}
    from_iso8601_date(format('%04d-%02d-%02d', {{ anio }}, {{ mes }}, {{ dia }}))
{%- endmacro %}


{% macro date_format(fecha, formato) -%}
    {{ return(adapter.dispatch('date_format', 'gold')(fecha, formato)) }}
{%- endmacro %}

{% macro default__date_format(fecha, formato) -%}
    date_format({{ fecha }}, '{{ formato }}')
{%- endmacro %}

{% macro athena__date_format(fecha, formato) -%}
    {# `date_format` existe en Trino pero con patrones de MySQL ('%Y-%m'). El que usa los
       mismos patrones de Java que Spark es `format_datetime`, y pide timestamp. #}
    format_datetime(cast({{ fecha }} as timestamp), '{{ formato }}')
{%- endmacro %}


{% macro fin_de_mes(fecha) -%}
    {{ return(adapter.dispatch('fin_de_mes', 'gold')(fecha)) }}
{%- endmacro %}

{% macro default__fin_de_mes(fecha) -%}
    last_day({{ fecha }})
{%- endmacro %}

{% macro athena__fin_de_mes(fecha) -%}
    last_day_of_month({{ fecha }})
{%- endmacro %}


{% macro dias_entre(desde, hasta) -%}
    {{ return(adapter.dispatch('dias_entre', 'gold')(desde, hasta)) }}
{%- endmacro %}

{% macro default__dias_entre(desde, hasta) -%}
    datediff({{ hasta }}, {{ desde }})
{%- endmacro %}

{% macro athena__dias_entre(desde, hasta) -%}
    date_diff('day', {{ desde }}, {{ hasta }})
{%- endmacro %}


{% macro meses_entre(desde, hasta) -%}
    {{ return(adapter.dispatch('meses_entre', 'gold')(desde, hasta)) }}
{%- endmacro %}

{% macro default__meses_entre(desde, hasta) -%}
    {# `months_between` devuelve decimales; acá las dos fechas son un día 1, así que el
       entero es exacto. #}
    int(months_between({{ hasta }}, {{ desde }}))
{%- endmacro %}

{% macro athena__meses_entre(desde, hasta) -%}
    date_diff('month', {{ desde }}, {{ hasta }})
{%- endmacro %}


{% macro serie_de_meses(desde, hasta, alias) -%}
    {{ return(adapter.dispatch('serie_de_meses', 'gold')(desde, hasta, alias)) }}
{%- endmacro %}

{% macro default__serie_de_meses(desde, hasta, alias) -%}
    select explode(sequence({{ desde }}, {{ hasta }}, interval 1 month)) as {{ alias }}
{%- endmacro %}

{% macro athena__serie_de_meses(desde, hasta, alias) -%}
    {# El equivalente de `explode` es `unnest`, que en Trino no va en el SELECT sino en el
       FROM; y el intervalo se escribe con la cantidad entre comillas. #}
    select {{ alias }} from unnest(sequence({{ desde }}, {{ hasta }}, interval '1' month)) as t({{ alias }})
{%- endmacro %}


{% macro texto(expresion) -%}
    {{ return(adapter.dispatch('texto', 'gold')(expresion)) }}
{%- endmacro %}

{% macro default__texto(expresion) -%}
    cast({{ expresion }} as string)
{%- endmacro %}

{% macro athena__texto(expresion) -%}
    {# Trino no tiene el tipo `string`. #}
    cast({{ expresion }} as varchar)
{%- endmacro %}


{% macro patron_espacios() -%}
    {{ return(adapter.dispatch('patron_espacios', 'gold')()) }}
{%- endmacro %}

{% macro default__patron_espacios() -%}
    '\\s+'
{%- endmacro %}

{% macro athena__patron_espacios() -%}
    {# No es una función sino un literal: Spark interpreta las secuencias de escape dentro de
       la comilla simple y Trino no, así que la misma expresión regular se escribe distinto. #}
    '\s+'
{%- endmacro %}
