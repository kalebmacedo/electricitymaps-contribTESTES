import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, TypedDict
from collections import Counter

MAX_VALUE_LENGTH = 200

REFERENCE_REGEX = re.compile(r'\{\{([a-zA-Z0-9_\-\.]+)\}\}')


class ValidationError(TypedDict):
    type: str
    message: str
    key: str
    file: str


class ValidationWarning(TypedDict):
    type: str
    message: str
    key: str


class ValidationResult(TypedDict):
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationWarning]
    summary: str


def _generate_summary(
    errors: List[ValidationError],
    warnings: List[ValidationWarning]
) -> str:
    """Gera resumo simples e previsível para os testes."""
    total_errors = len(errors)
    total_warnings = len(warnings)

    error_types = Counter(e['type'] for e in errors)
    warning_types = Counter(w['type'] for w in warnings)

    summary_lines = [f"Total: {total_errors} erros, {total_warnings} avisos"]

    if error_types:
        et = ', '.join(f"{t}({c})" for t, c in error_types.items())
        summary_lines.append(f"Erros por tipo: {et}")

    if warning_types:
        wt = ', '.join(f"{t}({c})" for t, c in warning_types.items())
        summary_lines.append(f"Avisos por tipo: {wt}")

    return '\n'.join(summary_lines)


def _traverse_and_validate(
    obj: Dict[str, Any],
    prefix: str,
    file_path: str,
    errors: List[ValidationError],
    warnings: List[ValidationWarning]
) -> None:
    """Percorre recursivamente o objeto de traduções e valida formatos e tamanhos."""

    if not isinstance(obj, dict):
        return

    if obj == {} and prefix:
        warnings.append({
            'type': 'EMPTY_OBJECT',
            'message': 'Objeto de tradução vazio.',
            'key': prefix.rstrip('.')
        })
        return

    for k, v in obj.items():
        current_key = f"{prefix}{k}"

        if isinstance(v, dict):
            _traverse_and_validate(v, f"{current_key}.", file_path, errors, warnings)

        elif isinstance(v, str):
            if v == '':
                errors.append({
                    'type': 'INVALID_FORMAT',
                    'message': "Valor de tradução vazio ('').",
                    'key': current_key,
                    'file': file_path
                })
            else:
                if len(v) > MAX_VALUE_LENGTH:
                    warnings.append({
                        'type': 'LONG_VALUE',
                        'message': f"Valor excede {MAX_VALUE_LENGTH} caracteres ({len(v)}).",
                        'key': current_key
                    })
        else:
            errors.append({
                'type': 'INVALID_FORMAT',
                'message': f"Valor deve ser string ou objeto, não {type(v).__name__}.",
                'key': current_key,
                'file': file_path
            })


def validate_translation_file(
    translations: Dict[str, Any],
    prefix: str = '',
    file_path: str = ''
) -> ValidationResult:
    """Valida estrutura e conteúdo básico de um arquivo de traduções."""
    errors: List[ValidationError] = []
    warnings: List[ValidationWarning] = []

    _traverse_and_validate(translations, prefix, file_path, errors, warnings)

    is_valid = len(errors) == 0
    summary = _generate_summary(errors, warnings)

    return {
        'is_valid': is_valid,
        'errors': errors,
        'warnings': warnings,
        'summary': summary
    }


def validate_no_missing_keys(
    used_keys: List[str],
    available_keys: List[str]
) -> List[ValidationError]:
    """Retorna erros para chaves usadas no código mas não presentes nas traduções."""
    available_set = set(available_keys)
    missing = sorted(k for k in used_keys if k not in available_set)

    errors: List[ValidationError] = [
        {
            'type': 'MISSING_KEY',
            'message': "Chave usada no código, mas ausente nos arquivos de tradução.",
            'key': key,
            'file': ''
        }
        for key in missing
    ]
    return errors


def validate_no_unused_keys(
    available_keys: List[str],
    used_keys: List[str]
) -> List[ValidationWarning]:
    """Retorna warnings para chaves que existem nas traduções mas não são usadas."""
    used_set = set(used_keys)
    unused = sorted(k for k in available_keys if k not in used_set)

    warnings: List[ValidationWarning] = [
        {
            'type': 'UNUSED_KEY',
            'message': "Chave presente nos arquivos, mas não utilizada no código (pode ser obsoleta).",
            'key': key
        }
        for key in unused
    ]
    return warnings


def extract_all_keys(
    obj: Dict[str, Any],
    prefix: str = ''
) -> List[str]:
    """Extrai todas as chaves com path completo ('a.b.c') cujos valores são strings (ignora non-strings)."""
    keys: List[str] = []

    if not isinstance(obj, dict):
        return keys

    for k, v in obj.items():
        current = f"{prefix}{k}"
        if isinstance(v, str):
            keys.append(current)
        elif isinstance(v, dict):
            keys.extend(extract_all_keys(v, f"{current}."))
    return keys


def _build_key_value_map(translations: Dict[str, Any]) -> Dict[str, str]:
    """Cria um mapa key -> string_value para todas as keys que tem string como valor."""
    kv: Dict[str, str] = {}

    def _get_value(obj: Dict[str, Any], path_parts: List[str]):
        cur = obj
        for p in path_parts:
            if not isinstance(cur, dict) or p not in cur:
                return None
            cur = cur[p]
        return cur

    for full_key in extract_all_keys(translations):
        parts = full_key.split('.')
        val = _get_value(translations, parts)
        if isinstance(val, str):
            kv[full_key] = val
    return kv


def validate_no_circular_references(
    translations: Dict[str, Any]
) -> List[ValidationError]:
    """
    Detecta exclusivamente referências circulares (CIRCULAR_REFERENCE).
    Referências quebradas NÃO geram erro, pois o en.json contém
    placeholders inexistentes que são válidos no ElectricityMap.
    """
    kv = _build_key_value_map(translations)

    errors: List[ValidationError] = []
    seen_cycles = set()

    def refs_in(value: str) -> List[str]:
        return REFERENCE_REGEX.findall(value or "")

    for start in kv:
        stack: List[str] = []
        on_path = set()

        def dfs(current):
            if current not in kv:
                return

            if current in on_path:
                cycle_start = stack.index(current)
                cycle = stack[cycle_start:] + [current]
                cycle_str = " -> ".join(cycle)

                if cycle_str not in seen_cycles:
                    seen_cycles.add(cycle_str)
                    errors.append({
                        'type': 'CIRCULAR_REFERENCE',
                        'message': f"Referência circular detectada: {cycle_str}",
                        'key': start,
                        'file': ''
                    })
                return

            on_path.add(current)
            stack.append(current)

            for ref in refs_in(kv[current]):
                dfs(ref)

            stack.pop()
            on_path.remove(current)

        for ref in refs_in(kv[start]):
            dfs(ref)

    return errors

def validate_no_deprecated_keys(
    used_keys: List[str],
    deprecated_keys: List[str],
    migration_map: Dict[str, str]
) -> List[ValidationWarning]:
    """Detecta uso de chaves depreciadas e sugere migração se houver map."""
    deprecated_set = set(deprecated_keys)
    used_deprecated = sorted(k for k in used_keys if k in deprecated_set)

    warnings: List[ValidationWarning] = []
    for key in used_deprecated:
        to = migration_map.get(key)
        msg = f"A chave depreciada '{key}' ainda está em uso."
        if to:
            msg += f" Migre para '{to}'."
        warnings.append({
            'type': 'DEPRECATED_KEY',
            'message': msg,
            'key': key
        })
    return warnings


def generate_validation_report(result: ValidationResult) -> str:
    """Gera um relatório textual simples contendo erros e avisos — formatação esperada pelos testes."""
    report_lines: List[str] = []
    report_lines.append("RELATÓRIO DE VALIDAÇÃO DE INTEGRIDADE\n")

    if result['is_valid']:
        report_lines.append("STATUS: VÁLIDO\n")
    else:
        report_lines.append("STATUS: INVÁLIDO\n")

    report_lines.append(result.get('summary', '') + "\n")
    report_lines.append("─" * 70 + "\n")

    if result['errors']:
        report_lines.append(f"ERROS ({len(result['errors'])}):\n")
        for e in result['errors']:
            report_lines.append(f"*[{e['type']}] {e['message']}")
            if e.get('key'):
                report_lines.append(f"   Chave: {e['key']}")
            if e.get('file'):
                report_lines.append(f"   Arquivo: {e['file']}")
            report_lines.append("")
    else:
        report_lines.append("Nenhum erro encontrado\n")

    if result['warnings']:
        report_lines.append(f"AVISOS ({len(result['warnings'])}):\n")
        for w in result['warnings']:
            report_lines.append(f"*[{w['type']}] {w['message']}")
            if w.get('key'):
                report_lines.append(f"   Chave: {w['key']}")
            report_lines.append("")
    else:
        report_lines.append("Nenhum aviso\n")

    return "\n".join(report_lines)


def run_full_validation(
    translations: Dict[str, Any],
    used_keys_in_code: List[str],
    migration_map: Dict[str, str] = None
) -> ValidationResult:
    """Orquestra as validações e agrega resultados."""
    all_errors: List[ValidationError] = []
    all_warnings: List[ValidationWarning] = []

    available_keys = extract_all_keys(translations)

    file_res = validate_translation_file(translations)
    all_errors.extend(file_res['errors'])
    all_warnings.extend(file_res['warnings'])

    missing = validate_no_missing_keys(used_keys_in_code, available_keys)
    all_errors.extend(missing)

    unused = validate_no_unused_keys(available_keys, used_keys_in_code)
    all_warnings.extend(unused)

    ref_errors = validate_no_circular_references(translations)
    all_errors.extend(ref_errors)

    if migration_map:
        deprecated_keys = list(migration_map.keys())
        dep_warnings = validate_no_deprecated_keys(used_keys_in_code, deprecated_keys, migration_map)
        all_warnings.extend(dep_warnings)

    is_valid = len(all_errors) == 0
    summary = _generate_summary(all_errors, all_warnings)

    return {
        'is_valid': is_valid,
        'errors': all_errors,
        'warnings': all_warnings,
        'summary': summary
    }


def export_validation_to_json(
    result: ValidationResult,
    output_path: str
) -> None:
    """Exporta resultado (formato simples contendo is_valid, errors, warnings e summary)."""
    data = {
        'generated_at': datetime.now(timezone.utc).isoformat() + 'Z',
        'is_valid': result['is_valid'],
        'errors': result['errors'],
        'warnings': result['warnings'],
        'summary': result.get('summary', '')
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    en_file = Path(__file__).resolve().parents[6] / 'web' / 'src' / 'locales' / 'en.json'
    if not en_file.exists():
        print(f"Arquivo não encontrado: {en_file}")
        return

    with open(en_file, 'r', encoding='utf-8') as f:
        en_translations = json.load(f)

    used = extract_all_keys(en_translations)
    migration_map = {}
    result = run_full_validation(en_translations, used, migration_map)
    print(generate_validation_report(result))
    export_validation_to_json(result, str(Path.cwd() / 'validation-report.json'))


if __name__ == '__main__':
    main()