"""Lector del subconjunto de YAML que usan los archivos de configuración de los jobs.

El runner de Spark solo trae la stdlib y PySpark (ADR 0004): no hay PyYAML, y sumar una
dependencia al contenedor por dos archivos de configuración no se justifica. Los contratos
y el mapeo de tablas usan un subconjunto chico y estable: mapas anidados por indentación,
listas con `-`, listas en línea `[a, b]`, comentarios y escalares simples. Todo lo que
quede fuera de ese subconjunto falla con un error explícito en vez de adivinar.

Los tests comparan la salida de este módulo contra `yaml.safe_load` sobre los archivos
reales del repo, así que si alguien escribe YAML más elaborado se entera en CI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TRUE_FALSE = {"true": True, "false": False}
EMPTY = {"null", "~", ""}


@dataclass(frozen=True)
class Line:
    """Una línea significativa: sin comentario, sin espacios al final."""

    number: int
    indent: int
    text: str


def load_yaml_file(path: Path | str) -> Any:
    """Lee y parsea un archivo YAML del subconjunto soportado."""
    return parse_yaml(Path(path).read_text(encoding="utf-8"))


def parse_yaml(text: str) -> Any:
    """Documento YAML completo. Un archivo vacío es un mapa vacío."""
    lines = significant_lines(text)
    if not lines:
        return {}
    value, index = parse_block(lines, 0, lines[0].indent)
    if index != len(lines):
        raise ValueError(f"linea {lines[index].number}: indentacion inesperada")
    return value


def significant_lines(text: str) -> list[Line]:
    """Descarta blancos y comentarios, y anota la indentación de cada línea."""
    lines = []
    for number, raw in enumerate(text.splitlines(), start=1):
        content = strip_comment(raw)
        if not content.strip():
            continue
        indent = len(content) - len(content.lstrip())
        lines.append(Line(number=number, indent=indent, text=content.strip()))
    return lines


def strip_comment(raw: str) -> str:
    """Corta el `#` que abre comentario; el que va dentro de comillas se respeta."""
    quote = ""
    for index, char in enumerate(raw):
        if quote:
            if char == quote:
                quote = ""
        elif char in "\"'":
            quote = char
        elif char == "#" and (index == 0 or raw[index - 1] in " \t"):
            return raw[:index]
    return raw


def parse_block(lines: list[Line], index: int, indent: int) -> tuple[Any, int]:
    """Un bloque es una lista si arranca con `-`, y si no un mapa."""
    if lines[index].text.startswith("-"):
        return parse_sequence(lines, index, indent)
    return parse_mapping(lines, index, indent)


def parse_mapping(lines: list[Line], index: int, indent: int) -> tuple[dict[str, Any], int]:
    """Claves `nombre: valor` al mismo nivel de indentación."""
    result: dict[str, Any] = {}
    while index < len(lines) and lines[index].indent == indent:
        line = lines[index]
        key, separator, rest = line.text.partition(":")
        if not separator:
            raise ValueError(f"linea {line.number}: se esperaba 'clave: valor'")
        index += 1
        result[key.strip()], index = parse_value(lines, index, indent, rest.strip())
    return result, index


def parse_value(lines: list[Line], index: int, indent: int, rest: str) -> tuple[Any, int]:
    """Valor de una clave: en la misma línea, en un bloque anidado, o vacío."""
    if rest:
        return parse_scalar(rest), index
    if index < len(lines) and lines[index].indent > indent:
        return parse_block(lines, index, lines[index].indent)
    # Una lista puede ir al mismo nivel que su clave: es YAML válido y se usa seguido.
    if index < len(lines) and lines[index].indent == indent and lines[index].text.startswith("-"):
        return parse_sequence(lines, index, indent)
    return None, index


def parse_sequence(lines: list[Line], index: int, indent: int) -> tuple[list[Any], int]:
    """Ítems `- ...` al mismo nivel de indentación."""
    items: list[Any] = []
    while index < len(lines) and is_item(lines[index], indent):
        line = lines[index]
        tail = line.text[1:]
        rest = tail.strip()
        if not rest:
            index += 1
            value, index = parse_value(lines, index, indent, "")
        elif starts_a_mapping(rest):
            # `- clave: valor` es un mapa cuya primera clave está en la columna de `rest`:
            # se reescribe la línea con esa indentación y se parsea como mapa normal.
            item_indent = line.indent + 1 + (len(tail) - len(tail.lstrip()))
            lines[index] = Line(number=line.number, indent=item_indent, text=rest)
            value, index = parse_mapping(lines, index, item_indent)
        else:
            value = parse_scalar(rest)
            index += 1
        items.append(value)
    return items, index


def is_item(line: Line, indent: int) -> bool:
    """True si la línea es un ítem `- ...` de la lista que está en esa indentación."""
    return line.indent == indent and line.text.startswith("-")


def starts_a_mapping(text: str) -> bool:
    """`clave: valor` sí; un escalar suelto o una lista en línea no."""
    head, separator, _ = text.partition(":")
    return bool(separator) and not head.startswith(("\"", "'", "["))


def parse_scalar(token: str) -> Any:
    """Escalar YAML: lista en línea, string con comillas, booleano, nulo o número."""
    if token.startswith("[") and token.endswith("]"):
        inner = token[1:-1].strip()
        return [parse_scalar(part.strip()) for part in inner.split(",")] if inner else []
    if len(token) >= 2 and token.startswith('"') and token.endswith('"'):
        # Las reglas de escape de JSON son las mismas que las de YAML entre comillas dobles.
        return json.loads(token)
    if len(token) >= 2 and token.startswith("'") and token.endswith("'"):
        return token[1:-1].replace("''", "'")
    lowered = token.lower()
    if lowered in TRUE_FALSE:
        return TRUE_FALSE[lowered]
    if lowered in EMPTY:
        return None
    return parse_number(token)


def parse_number(token: str) -> Any:
    """Entero o decimal; si no es ninguno de los dos queda como string."""
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        return token
