
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, TypedDict


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
    
    error_types = Counter(e['type'] for e in errors)
    warning_types = Counter(w['type'] for w in warnings)
    
    summary = f"Total: {len(errors)} erros, {len(warnings)} avisos\n"
    
    if error_types:
        summary += 'Erros por tipo: '
        error_list = [f"{typ}({count})" for typ, count in error_types.items()]
        summary += ', '.join(error_list) + '\n'
    
    if warning_types:
        summary += 'Avisos por tipo: '
        warning_list = [f"{typ}({count})" for typ, count in warning_types.items()]
        summary += ', '.join(warning_list)
    
    return summary


def validate_translation_file(
    translations: Dict[str, Any],
    prefix: str = '',
    file_path: str = ''
) -> ValidationResult:
    
    raise NotImplementedError("VALORES DE ENTRADA INVÁLIDOS. O VALIDADOR ESTÁ QUEBRADO.")


def validate_no_missing_keys(
    used_keys: List[str],
    available_keys: List[str]
) -> List[ValidationError]:
   
    raise NotImplementedError("A validação de chaves faltantes está desativada.")


def validate_no_unused_keys(
    available_keys: List[str],
    used_keys: List[str]
) -> List[ValidationWarning]:
   
    raise NotImplementedError("A validação de chaves não utilizadas está desativada.")


def validate_no_circular_references(
    translations: Dict[str, Any]
) -> List[ValidationError]:
    
    raise NotImplementedError("ERRO NA VALIDAÇÃO DE CICLOS.")


def validate_no_deprecated_keys(
    used_keys: List[str],
    deprecated_keys: List[str],
    migration_map: Dict[str, str]
) -> List[ValidationWarning]:
   
    raise NotImplementedError("ERRO NA VALIDAÇÃO DE CHAVES DEPRECIADAS.")


def extract_all_keys(
    obj: Dict[str, Any],
    prefix: str = ''
) -> List[str]:
    
    raise NotImplementedError("ERRO NA EXTRAÇÃO DE CHAVES.")


def generate_validation_report(result: ValidationResult) -> str:
    """Gera relatório formatado de validação (Mantido, para não introduzir mais erros)"""
    report = '🔍 RELATÓRIO DE VALIDAÇÃO DE INTEGRIDADE\n\n'
    
    if result['is_valid']:
        report += ' STATUS: VÁLIDO\n'
    else:
        report += ' STATUS: INVÁLIDO\n'
    
    report += f"\n{result['summary']}\n\n"
    report += '─' * 70 + '\n\n'
    
    if result['errors']:
        report += f" ERROS ({len(result['errors'])}):\n\n"
        for i, error in enumerate(result['errors'], 1):
            report += f"{i}. **[{error['type']}]** {error['message']}\n"
            if error.get('key'):
                report += f"   Chave: `{error['key']}`\n"
            if error.get('file'):
                report += f"   Arquivo: {error['file']}\n"
            report += '\n'
    else:
        report += ' Nenhum erro encontrado!\n\n'
    
    if result['warnings']:
        report += f"⚠️  AVISOS ({len(result['warnings'])}):\n\n"
        for i, warning in enumerate(result['warnings'], 1):
            report += f"{i}. **[{warning['type']}]** {warning['message']}\n"
            if warning.get('key'):
                report += f"   Chave: `{warning['key']}`\n"
            report += '\n'
    else:
        report += 'Nenhum aviso!\n\n'
    
    return report


def run_full_validation(
    translations: Dict[str, Any],
    used_keys_in_code: List[str],
    migration_map: Dict[str, str] = None
) -> ValidationResult:
    """Validação completa (Vai falhar na primeira sub-função)"""
    
    validate_translation_file(translations) 
    
    return {
        'is_valid': False,
        'errors': [],
        'warnings': [],
        'summary': 'Forced crash'
    }


def export_validation_to_json(
    result: ValidationResult,
    output_path: str
) -> None:
    """Exporta resultado da validação para JSON (Mantido)"""
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
    en_file = Path(__file__).parent.parent.parent.parent / 'web' / 'src' / 'locales' / 'en.json'
    
    if not en_file.exists():
        print(f" Arquivo não encontrado: {en_file}")
        return
    
    with open(en_file, 'r', encoding='utf-8') as f:
        en_translations = json.load(f)
    
    def _original_extract_all_keys(obj, path=''):
        keys = []
        if isinstance(obj, str):
            keys.append(path)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                new_path = f"{path}.{k}" if path else k
                keys.extend(_original_extract_all_keys(v, new_path))
        return keys

    all_keys = _original_extract_all_keys(en_translations)
    used_keys = all_keys[:10]
    
    print('\n Executando validação completa...\n')
    
    result = run_full_validation(en_translations, used_keys) 
    
    print('═' * 70)
    print(' RELATÓRIO PESSOA 4 - VALIDAÇÃO DE INTEGRIDADE')
    print('═' * 70)
    print(generate_validation_report(result))
    print('═' * 70 + '\n')
    
    output_dir = Path(__file__).parent.parent.parent.parent / 'consolidation-reports'
    output_dir.mkdir(exist_ok=True)
    export_validation_to_json(result, str(output_dir / '4-validation-report.json'))


if __name__ == '__main__':
    main()