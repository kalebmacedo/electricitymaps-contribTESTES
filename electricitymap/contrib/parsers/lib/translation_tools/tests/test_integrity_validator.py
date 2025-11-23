import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from electricitymap.contrib.parsers.lib.translation_tools.integrity_validator import (
    ValidationError,
    ValidationResult,
    ValidationWarning,
    export_validation_to_json,
    extract_all_keys,
    generate_validation_report,
    run_full_validation,
    validate_no_circular_references,
    validate_no_deprecated_keys,
    validate_no_missing_keys,
    validate_no_unused_keys,
    validate_translation_file,
    MAX_VALUE_LENGTH
)


class TestIntegrityValidator:
    """Testes para o validador de integridade"""

    @pytest.fixture
    def en_translations(self) -> Dict[str, Any]:
        """Carrega o arquivo de traduções EN para testes"""
        repo_root = Path(__file__).resolve().parents[6]
        translation_file = repo_root / 'web' / 'src' / 'locales' / 'en.json'
        if translation_file.exists():
            with open(translation_file, 'r', encoding='utf-8') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

    def test_validate_translation_file_passes_valid_structure(self):
        """Deve validar arquivo sem erros."""
        valid_translations = {
            'section': {
                'key1': 'Value 1',
                'key2': 'Value 2'
            }
        }

        result = validate_translation_file(valid_translations)

        assert result['is_valid'] is True
        assert len(result['errors']) == 0

    def test_validate_translation_file_detects_empty_strings(self):
        """Deve detectar valores vazios."""
        invalid_translations = {
            'section': {
                'key1': '',
                'key2': 'Valid value'
            }
        }

        result = validate_translation_file(invalid_translations)

        assert len(result['errors']) == 1
        assert result['errors'][0]['type'] == 'INVALID_FORMAT'
        assert result['errors'][0]['key'] == 'section.key1'

    def test_validate_translation_file_detects_non_strings(self):
        """Deve detectar valores não-string."""
        invalid_translations = {
            'section': {
                'key1': 123,
                'key2': True,
                'key3': None
            }
        }

        result = validate_translation_file(invalid_translations)

        assert len(result['errors']) == 3
        assert all(e['type'] == 'INVALID_FORMAT' for e in result['errors'])

    def test_validate_translation_file_detects_empty_objects(self):
        """Deve detectar objetos vazios."""
        invalid_translations = {
            'section': {},
            'validSection': {
                'key': 'value'
            }
        }

        result = validate_translation_file(invalid_translations)

        assert len(result['warnings']) == 1
        assert result['warnings'][0]['type'] == 'EMPTY_OBJECT'
        assert result['warnings'][0]['key'] == 'section'

    def test_validate_translation_file_warns_about_long_values(self):
        """Deve avisar sobre valores muito longos."""
        long_value = 'a' * (MAX_VALUE_LENGTH + 1)
        translations = {
            'key': long_value
        }

        result = validate_translation_file(translations)

        long_value_warning = next(
            (w for w in result['warnings'] if w['type'] == 'LONG_VALUE'),
            None
        )
        assert long_value_warning is not None
        assert long_value_warning['key'] == 'key'
        assert result['is_valid'] is True

    def test_validate_no_missing_keys_detects_missing(self):
        """Deve detectar chaves usadas mas não definidas."""
        used_keys = ['key1', 'key2', 'key3']
        available_keys = ['key1', 'key2']

        errors = validate_no_missing_keys(used_keys, available_keys)

        assert len(errors) == 1
        assert errors[0]['key'] == 'key3'
        assert errors[0]['type'] == 'MISSING_KEY'

    def test_validate_no_missing_keys_passes_when_all_exist(self):
        """Deve passar quando todas chaves existem."""
        used_keys = ['key1', 'key2']
        available_keys = ['key1', 'key2', 'key3']

        errors = validate_no_missing_keys(used_keys, available_keys)

        assert len(errors) == 0

    def test_validate_no_missing_keys_handles_nested_keys(self):
        """Deve lidar com chaves aninhadas."""
        used_keys = ['section.subsection.key']
        available_keys = ['section.subsection.key']

        errors = validate_no_missing_keys(used_keys, available_keys)

        assert len(errors) == 0

    def test_validate_no_unused_keys_detects_unused(self):
        """Deve detectar chaves definidas mas nunca usadas."""
        available_keys = ['key1', 'key2', 'key3']
        used_keys = ['key1']

        warnings = validate_no_unused_keys(available_keys, used_keys)

        assert len(warnings) == 2
        assert any(w['key'] == 'key2' for w in warnings)
        assert any(w['key'] == 'key3' for w in warnings)

    def test_validate_no_unused_keys_passes_when_all_used(self):
        """Não deve gerar warnings quando todas chaves são usadas."""
        available_keys = ['key1', 'key2']
        used_keys = ['key1', 'key2']

        warnings = validate_no_unused_keys(available_keys, used_keys)

        assert len(warnings) == 0

    def test_validate_no_circular_references_detects_simple_circular(self):
        """Deve detectar referências circulares simples."""
        translations = {
            'key1': 'Value with {{key2}}',
            'key2': 'Value with {{key1}}'
        }

        errors = validate_no_circular_references(translations)
        assert len(errors) == 2
        assert errors[0]['type'] == 'CIRCULAR_REFERENCE'
        assert errors[1]['type'] == 'CIRCULAR_REFERENCE'
        
        error_messages = [e['message'] for e in errors]
        assert 'Referência circular detectada: key1 -> key2 -> key1' in error_messages
        assert 'Referência circular detectada: key2 -> key1 -> key2' in error_messages

    def test_validate_no_circular_references_passes_valid(self):
        """Deve passar quando não há referências circulares."""
        translations = {
            'key1': 'Simple value',
            'key2': 'Another value'
        }

        errors = validate_no_circular_references(translations)

        assert len(errors) == 0

    def test_validate_no_circular_references_allows_valid_refs(self):
        """Deve permitir referências válidas não-circulares."""
        translations = {
            'key1': 'Hello {{key2}}',
            'key2': 'Goodbye World'
        }

        errors = validate_no_circular_references(translations)

        assert len(errors) == 0

    def test_validate_no_deprecated_keys_detects_usage(self):
        """Deve detectar uso de chaves depreciadas."""
        used_keys = ['old.key', 'new.key']
        deprecated_keys = ['old.key']
        migration_map = {'old.key': 'new.key'}

        warnings = validate_no_deprecated_keys(
            used_keys,
            deprecated_keys,
            migration_map
        )

        assert len(warnings) == 1
        assert warnings[0]['type'] == 'DEPRECATED_KEY'
        assert warnings[0]['key'] == 'old.key'
        assert 'new.key' in warnings[0]['message']

    def test_validate_no_deprecated_keys_passes_valid(self):
        """Não deve gerar warnings para chaves não-depreciadas."""
        used_keys = ['valid.key']
        deprecated_keys = ['old.key']
        migration_map = {'old.key': 'new.key'}

        warnings = validate_no_deprecated_keys(
            used_keys,
            deprecated_keys,
            migration_map
        )

        assert len(warnings) == 0

    def test_extract_all_keys_simple_object(self):
        """Deve extrair todas as chaves de objeto simples."""
        obj = {
            'key1': 'value1',
            'key2': 'value2'
        }

        keys = extract_all_keys(obj)

        assert sorted(keys) == ['key1', 'key2']

    def test_extract_all_keys_nested_object(self):
        """Deve extrair chaves de objetos aninhados com path completo."""
        obj = {
            'section1': {
                'subsection': {
                    'key1': 'value'
                }
            },
            'section2': {
                'key2': 'value'
            },
            'section3': 123
        }

        keys = extract_all_keys(obj)

        assert 'section1.subsection.key1' in keys
        assert 'section2.key2' in keys
        assert 'section3' not in keys
        assert len(keys) == 2

    def test_extract_all_keys_ignores_non_strings(self):
        """Deve ignorar valores não-string, exceto se forem dicts aninhados."""
        obj = {
            'key1': 'string value',
            'key2': 123,
            'key3': {'nested': 'value'}
        }

        keys = extract_all_keys(obj)

        assert 'key1' in keys
        assert 'key3.nested' in keys
        assert 'key2' not in keys
        assert len(keys) == 2

    def test_generate_validation_report_success(self):
        """Deve gerar relatório para validação bem-sucedida."""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': 'All good!'
        }

        report = generate_validation_report(result)

        assert 'STATUS: VÁLIDO' in report
        assert 'STATUS: INVÁLIDO' not in report
        assert 'Nenhum erro encontrado' in report
        assert 'Nenhum aviso' in report


    def test_generate_validation_report_with_errors(self):
        """Deve gerar relatório com erros e warnings."""
        result = {
            'is_valid': False,
            'errors': [
                {
                    'type': 'MISSING_KEY',
                    'message': 'Key not found',
                    'key': 'test.key',
                    'file': 'file.json'
                }
            ],
            'warnings': [
                {
                    'type': 'UNUSED_KEY',
                    'message': 'Key not used',
                    'key': 'unused.key'
                }
            ],
            'summary': 'Problems found'
        }

        report = generate_validation_report(result)

        assert 'STATUS: INVÁLIDO' in report
        assert 'ERROS (1)' in report
        assert 'AVISOS (1)' in report
        assert 'MISSING_KEY' in report
        assert 'UNUSED_KEY' in report
        assert 'file.json' in report

    def test_run_full_validation_complete(self):
        """Deve executar validação completa e detectar todos os tipos de problema."""
        translations = {
            'key1': 'Value 1',
            'key2': 'Value with {{missing_ref}}',
            'key3': 'Value with {{key4}}',
            'key4': 'Value with {{key3}}',
            'unused.key': 'I am unused',
            'long.key': 'a' * (MAX_VALUE_LENGTH + 1),
            'deprecated.key': 'Old value'
        }
        used_keys = ['key1', 'key2', 'key3', 'missing.key', 'deprecated.key']
        migration_map = {'deprecated.key': 'new.key'}

        result = run_full_validation(translations, used_keys, migration_map)

        assert result['is_valid'] is False

        error_types = [e['type'] for e in result['errors']]
        assert error_types.count('CIRCULAR_REFERENCE') == 2
        assert 'MISSING_KEY' in error_types
        assert 'BROKEN_REFERENCE' not in error_types

        warning_types = [w['type'] for w in result['warnings']]
        assert 'UNUSED_KEY' in warning_types
        assert 'LONG_VALUE' in warning_types
        assert 'DEPRECATED_KEY' in warning_types

    def test_run_full_validation_with_migration_map(self):
        """Deve validar com mapa de migração."""
        translations = {
            'new.key': 'Value',
            'unused.key': 'Value',
            'deprecated.key': 'Old value'
        }
        used_keys = ['deprecated.key', 'new.key']
        migration_map = {'deprecated.key': 'new.key'}

        result = run_full_validation(translations, used_keys, migration_map)
        assert result['is_valid'] is True
        assert len(result['errors']) == 0
        
        warning_types = [w['type'] for w in result['warnings']]
        assert 'DEPRECATED_KEY' in warning_types
        assert 'UNUSED_KEY' in warning_types
        
        deprecated_warning = next(
            (w for w in result['warnings'] if w['type'] == 'DEPRECATED_KEY'),
            None
        )
        assert deprecated_warning is not None
        assert deprecated_warning['key'] == 'deprecated.key'

    def test_export_validation_to_json_creates_file(self, tmp_path):
        """Deve exportar resultado da validação para JSON."""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'summary': 'OK'
        }
        output_file = tmp_path / 'validation.json'

        export_validation_to_json(result, str(output_file))

        assert output_file.exists()

        with open(output_file, 'r') as f:
            data = json.load(f)
            assert 'is_valid' in data
            assert data['is_valid'] is True

    def test_validate_real_en_json_structure(self, en_translations):
        """Deve validar estrutura do arquivo en.json real."""
        if not en_translations:
            pytest.skip("Arquivo en.json não encontrado ou vazio/inválido.")

        result = validate_translation_file(en_translations)

        assert len(result['errors']) == 0

    def def_extract_keys_from_real_en_json(self, en_translations):
        """Deve extrair todas as chaves do en.json."""
        if not en_translations:
            pytest.skip("Arquivo en.json não encontrado ou vazio/inválido.")

        keys = extract_all_keys(en_translations)

        assert len(keys) > 0
        assert isinstance(keys, list)

    def test_validate_no_circular_refs_in_real_en_json(self, en_translations):
        """Deve validar que não há referências circulares ou quebradas no en.json."""
        if not en_translations:
            pytest.skip("Arquivo en.json não encontrado ou vazio/inválido.")

        errors = validate_no_circular_references(en_translations)

        assert len(errors) == 0


class TestIntegrityValidatorIntegration:
    """Testes de integração com outras pessoas."""

    def test_integration_complete_migration_scenario(self):
        """Deve validar cenário completo de consolidação."""
        mock_translations = {
            'canonical': {
                'key': 'Shared value'
            },
            'section': {
                'nested': 'Another value'
            },
            'unused.key': 'I am unused',
            'deprecated': {
                'key': 'Old value'
            }
        }

        mock_used_keys = [
            'canonical.key',
            'deprecated.key',
            'section.nested'
        ]

        mock_migration_map = {
            'deprecated.key': 'canonical.key'
        }

        result = run_full_validation(
            mock_translations,
            mock_used_keys,
            mock_migration_map
        )

        assert result['is_valid'] is True
        assert len(result['errors']) == 0

        warning_types = [w['type'] for w in result['warnings']]
        assert 'DEPRECATED_KEY' in warning_types
        assert 'UNUSED_KEY' in warning_types


class TestIntegrityValidatorEdgeCases:
    """Testes de casos extremos."""

    def test_handles_deeply_nested_structures(self):
        """Deve lidar com estruturas profundamente aninhadas."""
        deep_obj = {'l1': {'l2': {'l3': {'l4': {'key': 'value'}}}}}

        keys = extract_all_keys(deep_obj)

        assert 'l1.l2.l3.l4.key' in keys
        assert len(keys) == 1

    def test_handles_unicode_in_keys(self):
        """Deve lidar com Unicode nas chaves."""
        obj = {
            'key_with_émojis_🎉': 'value',
            'chave_em_português': 'valor'
        }

        result = validate_translation_file(obj)

        assert result['is_valid'] is True