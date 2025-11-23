
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, TypedDict
from collections import Counter

# Constantes de Validação
MAX_VALUE_LENGTH = 200

# Regex para encontrar referências no formato {{chave}}
REFERENCE_REGEX = re.compile(r'\{\{([a-zA-Z0-9_\-.]+)\}\}')


class ValidationError(TypedDict):
    """Tipo para erro de validação"""
    type: str  # 'MISSING_KEY', 'BROKEN_REFERENCE', 'CIRCULAR_REFERENCE', 'INVALID_FORMAT'
    message: str
    key: str
    file: str


class ValidationWarning(TypedDict):
    """Tipo para aviso de validação"""
    type: str  # 'UNUSED_KEY', 'DEPRECATED_KEY', 'LONG_VALUE', 'EMPTY_OBJECT'
    message: str
    key: str


class ValidationResult(TypedDict):
    """Tipo para resultado de validação"""
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationWarning]
    summary: str


def _generate_summary(
    errors: List[ValidationError],
    warnings: List[ValidationWarning]
) -> str:
    """Gera resumo executivo privado para o resultado"""

    error_types = Counter(e['type'] for e in errors)
    warning_types = Counter(w['type'] for w in warnings)

    summary = f"Total: {len(errors)} erros, {len(warnings)} avisos\n"

    if error_types:
        summary += 'Erros por tipo: '
        error_list = [f"{typ}(count)" for typ, count in error_types.items()]
        summary += ', '.join(error_list) + '\n'

    if warning_types:
        summary += 'Avisos por tipo: '
        warning_list = [f"{typ}(count)" for typ, count in warning_types.items()]
        summary += ', '.join(warning_list)

    return summary


def _traverse_and_validate(
    obj: Dict[str, Any],
    prefix: str,
    file_path: str,
    errors: List[ValidationError],
    warnings: List[ValidationWarning]
) -> None:
    """Função recursiva para percorrer e validar chaves/valores de um arquivo de tradução."""

    if not isinstance(obj, dict):
        return

    # Correção da indentação na linha 85
    if not obj and prefix:
         warnings.append({
            'type': 'EMPTY_OBJECT',
            'message': f"Objeto de tradução vazio encontrado.",
            'key': prefix.rstrip('.')
        })
         return

    for key, value in obj.items():
        current_key = f"{prefix}{key}"

        if isinstance(value, dict):
            _traverse_and_validate(value, f"{current_key}.", file_path, errors, warnings)

        elif isinstance(value, str):
            if not value:
                errors.append({
                    'type': 'INVALID_FORMAT',
                    'message': "Valor de tradução vazio ('').",
                    'key': current_key,
                    'file': file_path
                })
            elif len(value) > MAX_VALUE_LENGTH:
                warnings.append({
                    'type': 'LONG_VALUE',
                    'message': f"Valor de tradução excede {MAX_VALUE_LENGTH} caracteres ({len(value)}).",
                    'key': current_key
                })

        else:
            errors.append({
                'type': 'INVALID_FORMAT',
                'message': f"Valor de tradução deve ser string ou objeto, não {type(value).__name__}.",
                'key': current_key,
                'file': file_path
            })


def validate_translation_file(
    translations: Dict[str, Any],
    prefix: str = '',
    file_path: str = ''
) -> ValidationResult:
    """Valida a estrutura interna do arquivo de traduções."""
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
    """Identifica chaves usadas no código mas ausentes nos arquivos de tradução."""
    available_set = set(available_keys)
    missing_keys = set(used_keys) - available_set

    errors: List[ValidationError] = [
        {
            'type': 'MISSING_KEY',
            'message': f"Chave usada no código, mas ausente nos arquivos de tradução.",
            'key': key,
            'file': '' # O arquivo específico seria determinado pelo orquestrador
        }
        for key in sorted(list(missing_keys))
    ]
    return errors


def validate_no_unused_keys(
    available_keys: List[str],
    used_keys: List[str]
) -> List[ValidationWarning]:
    """Identifica chaves definidas nos arquivos de tradução mas não usadas no código."""
    used_set = set(used_keys)
    unused_keys = set(available_keys) - used_set

    warnings: List[ValidationWarning] = [
        {
            'type': 'UNUSED_KEY',
            'message': "Chave presente nos arquivos, mas não utilizada no código (pode ser chave morta).",
            'key': key
        }
        for key in sorted(list(unused_keys))
    ]
    return warnings


def validate_no_circular_references(
    translations: Dict[str, Any]
) -> List[ValidationError]:
    """Identifica referências circulares e referências quebradas."""

    # 1. Extrai todas as chaves e seus valores (para simplificar a busca)
    all_keys = extract_all_keys(translations)
    all_key_values = {}

    def _get_value_by_key(obj, key_path):
        parts = key_path.split('.')
        current = obj
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    for key in all_keys:
        value = _get_value_by_key(translations, key)
        if isinstance(value, str):
            all_key_values[key] = value

    errors: List[ValidationError] = []

    # 2. Verifica ciclos para cada chave
    for start_key in all_key_values:

        def find_references(value):
            return REFERENCE_REGEX.findall(value)

        def find_cycles(current_key, visited, path):
            # Condição de ciclo
            if current_key in visited:
                cycle_start_index = path.index(current_key)
                cycle = " -> ".join(path[cycle_start_index:] + [current_key])
                # Reporta o erro APENAS se o ciclo começar pelo start_key
                # Isso impede a duplicação no caso de A -> B -> A
                if path[cycle_start_index] == start_key:
                    errors.append({
                        'type': 'CIRCULAR_REFERENCE',
                        'message': f"Referência circular detectada: {cycle}",
                        'key': start_key,
                        'file': ''
                    })
                return True

            if current_key not in all_key_values:
                # É uma referência quebrada, o erro é tratado na função find_cycles que chamou
                return False

            visited.add(current_key)
            path.append(current_key)

            value = all_key_values[current_key]

            for ref_key in find_references(value):
                # Se a chave referenciada não existe (é uma referência quebrada)
                if ref_key not in all_key_values:
                    errors.append({
                        'type': 'BROKEN_REFERENCE',
                        'message': f"Referência quebrada: '{current_key}' refere-se a chave inexistente '{{{{{ref_key}}}}}'",
                        'key': current_key, # Chave quebrada é o current_key
                        'file': ''
                    })
                    # Não continua a recursão nesta ref_key
                    continue

                # Se a referência não foi visitada
                if find_cycles(ref_key, visited, path):
                    # Se um ciclo foi encontrado, propagar o status
                    return True

            path.pop()
            visited.remove(current_key)
            return False

        # Verifica ciclos e referências quebradas, iniciando a busca em start_key
        find_cycles(start_key, set(), [])

    # Remove duplicatas de erros (principalmente BROKEN_REFERENCE, que pode ser detectado várias vezes)
    unique_errors = []
    seen = set()
    for error in errors:
        # A combinação de tipo, chave e mensagem deve ser única
        error_tuple = (error['type'], error['key'], error['message'])
        if error_tuple not in seen:
            unique_errors.append(error)
            seen.add(error_tuple)

    return unique_errors


def validate_no_deprecated_keys(
    used_keys: List[str],
    deprecated_keys: List[str],
    migration_map: Dict[str, str]
) -> List[ValidationWarning]:
    """Identifica o uso de chaves depreciadas no código."""

    deprecated_set = set(deprecated_keys)
    used_deprecated_keys = deprecated_set.intersection(set(used_keys))

    warnings: List[ValidationWarning] = [
        {
            'type': 'DEPRECATED_KEY',
            'message': f"A chave depreciada '{key}' ainda está em uso. Migre para '{migration_map.get(key)}'.",
            'key': key
        }
        for key in sorted(list(used_deprecated_keys))
    ]
    return warnings


def extract_all_keys(
    obj: Dict[str, Any],
    prefix: str = ''
) -> List[str]:
    """Extrai todas as chaves (paths completos) de um objeto de tradução aninhado."""
    keys: List[str] = []

    if not isinstance(obj, dict):
        return keys

    for k, v in obj.items():
        current_key = f"{prefix}{k}"

        # Se for um valor string, adiciona a chave completa
        if isinstance(v, str):
            keys.append(current_key)

        # Se for um dicionário, continua a recursão
        elif isinstance(v, dict):
            # Adiciona o prefixo correto para a recursão
            keys.extend(extract_all_keys(v, f"{current_key}."))

    return keys


# --- Funções de Relatório e Orquestração ---

def generate_validation_report(result: ValidationResult) -> str:
    """Gera relatório formatado de validação."""
    report = '🔍 RELATÓRIO DE VALIDAÇÃO DE INTEGRIDADE\n\n'

    if result['is_valid']:
        report += '✅ STATUS: VÁLIDO\n'
    else:
        report += '❌ STATUS: INVÁLIDO\n'

    report += f"\n{result['summary']}\n\n"
    report += '─' * 70 + '\n\n'

    if result['errors']:
        report += f"❌ ERROS ({len(result['errors'])}):\n\n"
        for i, error in enumerate(result['errors'], 1):
            report += f"{i}. *[{error['type']}]* {error['message']}\n"
            if error.get('key'):
                report += f"   Chave: {error['key']}\n"
            if error.get('file'):
                report += f"   Arquivo: {error['file']}\n"
            report += '\n'
    else:
        report += '✅ Nenhum erro encontrado!\n\n'

    if result['warnings']:
        report += f"⚠️  AVISOS ({len(result['warnings'])}):\n\n"
        for i, warning in enumerate(result['warnings'], 1):
            report += f"{i}. *[{warning['type']}]* {warning['message']}\n"
            if warning.get('key'):
                report += f"   Chave: {warning['key']}\n"
            report += '\n'
    else:
        report += '✅ Nenhum aviso!\n\n'

    return report


def run_full_validation(
    translations: Dict[str, Any],
    used_keys_in_code: List[str],
    migration_map: Dict[str, str] = None
) -> ValidationResult:
    """Orquestra a validação completa."""
    all_errors: List[ValidationError] = []
    all_warnings: List[ValidationWarning] = []

    # 1. Extrair todas as chaves disponíveis
    available_keys = extract_all_keys(translations)

    # 2. Validar estrutura do arquivo (formato, strings vazias, valores longos, objetos vazios)
    file_validation = validate_translation_file(translations)
    all_errors.extend(file_validation['errors'])
    all_warnings.extend(file_validation['warnings'])

    # 3. Validar chaves faltando (usadas no código, mas ausentes)
    missing_key_errors = validate_no_missing_keys(used_keys_in_code, available_keys)
    all_errors.extend(missing_key_errors)

    # 4. Validar chaves não usadas (presentes no arquivo, mas ausentes no código)
    unused_key_warnings = validate_no_unused_keys(available_keys, used_keys_in_code)
    all_warnings.extend(unused_key_warnings)

    # 5. Validar referências circulares e quebradas
    circular_errors = validate_no_circular_references(translations)
    all_errors.extend(circular_errors)

    # 6. Validar chaves depreciadas (se houver mapa de migração)
    if migration_map:
        deprecated_keys = list(migration_map.keys())
        deprecated_warnings = validate_no_deprecated_keys(
            used_keys_in_code,
            deprecated_keys,
            migration_map
        )
        all_warnings.extend(deprecated_warnings)

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
    """Exporta resultado da validação para JSON."""
    data = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'generated_by': 'Pessoa 4 - Validador de Integridade (Python)',
        'is_valid': result['is_valid'],
        'total_errors': len(result['errors']),
        'total_warnings': len(result['warnings']),
        'summary': result['summary'],
        'errors': result['errors'],
        'warnings': result['warnings']
    }

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ Validação exportada para: {output_path}")


def main():
    """Função principal para execução standalone."""
    # Simular o caminho para o arquivo de tradução (ajuste conforme a estrutura do projeto)
    en_file = Path(__file__).parent.parent.parent.parent.parent / 'web' / 'src' / 'locales' / 'en.json'

    if not en_file.exists():
        print(f"❌ Arquivo não encontrado: {en_file}")
        return

    with open(en_file, 'r', encoding='utf-8') as f:
        en_translations = json.load(f)

    # Extrai chaves para simular chaves usadas
    all_keys = extract_all_keys(en_translations)
    used_keys = all_keys

    print('\n🔍 Executando validação completa...\n')

    migration_map = {}

    result = run_full_validation(en_translations, used_keys, migration_map)

    print('═' * 70)
    print(' RELATÓRIO VALIDAÇÃO DE INTEGRIDADE')
    print('═' * 70)
    print(generate_validation_report(result))
    print('═' * 70 + '\n')

    output_dir = Path(__file__).parent.parent.parent.parent.parent / 'consolidation-reports'
    output_dir.mkdir(exist_ok=True)
    export_validation_to_json(result, str(output_dir / '4-validation-report.json'))


if __name__ == '_main_':
    main()