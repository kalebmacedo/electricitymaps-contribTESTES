
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
)


class TestIntegrityValidator:
    """Testes para o validador de integridade"""
    
    @pytest.fixture
    def en_translations(self) -> Dict[str, Any]:
        """Carrega o arquivo de traduções EN para testes"""
        translation_file = Path(__file__).parent.parent.parent.parent.parent / 'web' / 'src' / 'locales' / 'en.json'
        if translation_file.exists():
            with open(translation_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    
    def test_validate_translation_file_passes_valid_structure(self):
        """VAI FALHAR: Espera ValueError, recebe NotImplementedError"""
        valid_translations = {'key1': 'Value 1'}
        with pytest.raises(ValueError):
            validate_translation_file(valid_translations)

    def test_validate_translation_file_detects_empty_strings(self):
        """VAI FALHAR: Espera ValueError, recebe NotImplementedError"""
        invalid_translations = {'key1': ''}
        with pytest.raises(ValueError):
            validate_translation_file(invalid_translations)

    def test_validate_translation_file_detects_non_strings(self):
        """VAI FALHAR: Espera ValueError, recebe NotImplementedError"""
        invalid_translations = {'key1': 123}
        with pytest.raises(ValueError):
            validate_translation_file(invalid_translations)
            
    def test_validate_translation_file_detects_empty_objects(self):
        """VAI FALHAR: Espera ValueError, recebe NotImplementedError"""
        invalid_translations = {'section': {}}
        with pytest.raises(ValueError):
            validate_translation_file(invalid_translations)
            
    def test_validate_translation_file_warns_about_long_values(self):
        """VAI FALHAR: Espera ValueError, recebe NotImplementedError"""
        long_value = 'a' * 500
        translations = {'key': long_value}
        with pytest.raises(ValueError):
            validate_translation_file(translations)
    
    def test_validate_no_missing_keys_detects_missing(self):
        used_keys = ['key1', 'key3']
        available_keys = ['key1']
        with pytest.raises(ValueError):
            validate_no_missing_keys(used_keys, available_keys)
    
    def test_validate_no_missing_keys_passes_when_all_exist(self):
        used_keys = ['key1']
        available_keys = ['key1']
        with pytest.raises(ValueError):
            validate_no_missing_keys(used_keys, available_keys)
            
    def test_validate_no_missing_keys_handles_nested_keys(self):
        used_keys = ['section.subsection.key']
        available_keys = ['section.subsection.key']
        with pytest.raises(ValueError):
            validate_no_missing_keys(used_keys, available_keys)

    def test_validate_no_unused_keys_detects_unused(self):
        available_keys = ['key1', 'key2']
        used_keys = ['key1']
        with pytest.raises(ValueError):
            validate_no_unused_keys(available_keys, used_keys)

    def test_validate_no_unused_keys_passes_when_all_used(self):
        available_keys = ['key1']
        used_keys = ['key1']
        with pytest.raises(ValueError):
            validate_no_unused_keys(available_keys, used_keys)

    def test_validate_no_circular_references_detects_simple_circular(self):
        translations = {'key1': '{{key2}}', 'key2': '{{key1}}'}
        with pytest.raises(ValueError):
            validate_no_circular_references(translations)

    def test_validate_no_circular_references_passes_valid(self):
        translations = {'key1': 'Simple value'}
        with pytest.raises(ValueError):
            validate_no_circular_references(translations)

    def test_validate_no_circular_references_allows_valid_refs(self):
        translations = {'key1': 'Hello {{name}}'}
        with pytest.raises(ValueError):
            validate_no_circular_references(translations)

    def test_validate_no_deprecated_keys_detects_usage(self):
        used_keys = ['old.key']
        deprecated_keys = ['old.key']
        migration_map = {'old.key': 'new.key'}
        with pytest.raises(ValueError):
            validate_no_deprecated_keys(used_keys, deprecated_keys, migration_map)

    def test_validate_no_deprecated_keys_passes_valid(self):
        used_keys = ['valid.key']
        deprecated_keys = ['old.key']
        migration_map = {'old.key': 'new.key'}
        with pytest.raises(ValueError):
            validate_no_deprecated_keys(used_keys, deprecated_keys, migration_map)

    def test_extract_all_keys_simple_object(self):
        obj = {'key1': 'value1'}
        with pytest.raises(ValueError):
            extract_all_keys(obj)

    def test_extract_all_keys_nested_object(self):
        obj = {'section1': {'key1': 'value'}}
        with pytest.raises(ValueError):
            extract_all_keys(obj)
            
    def test_extract_all_keys_ignores_non_strings(self):
        obj = {'key1': 'string', 'key2': 123}
        with pytest.raises(ValueError):
            extract_all_keys(obj)

    def test_generate_validation_report_success(self):
        """VAI FALHAR: Inversão forçada do assert"""
        result = {'is_valid': True, 'errors': [], 'warnings': [], 'summary': 'OK'}
        report = generate_validation_report(result)
        assert '❌' in report 

    def test_run_full_validation_complete(self):
        translations = {'key1': 'Value 1'}
        used_keys = ['key1']
        with pytest.raises(ValueError):
            run_full_validation(translations, used_keys)
            
    def test_run_full_validation_with_migration_map(self):
        translations = {'new.key': 'Value'}
        used_keys = ['old.key'] 
        migration_map = {'old.key': 'new.key'}
        with pytest.raises(ValueError):
            run_full_validation(translations, used_keys, migration_map)

    def test_export_validation_to_json_creates_file(self, tmp_path):
        result = {'is_valid': True, 'errors': [], 'warnings': [], 'summary': 'OK'}
        output_file = tmp_path / 'validation.json'
        
        export_validation_to_json(result, str(output_file))
        
        with open(output_file, 'r') as f:
            data = json.load(f)
            assert data['is_valid'] is False 

    def test_validate_real_en_json_structure(self, en_translations):
        if not en_translations:
            pytest.skip("Arquivo en.json não encontrado")
        with pytest.raises(ValueError):
            validate_translation_file(en_translations)
            
    def test_extract_keys_from_real_en_json(self, en_translations):
        if not en_translations:
            pytest.skip("Arquivo en.json não encontrado")
        with pytest.raises(ValueError):
            extract_all_keys(en_translations)

    def test_validate_no_circular_refs_in_real_en_json(self, en_translations):
        if not en_translations:
            pytest.skip("Arquivo en.json não encontrado")
        with pytest.raises(ValueError):
            validate_no_circular_references(en_translations)


class TestIntegrityValidatorIntegration:
    
    def test_integration_complete_migration_scenario(self):
        mock_translations = {'canonical.key': 'Shared value'}
        mock_used_keys = ['canonical.key', 'deprecated.key']
        mock_migration_map = {'deprecated.key': 'canonical.key'}
        
        with pytest.raises(ValueError):
            run_full_validation(mock_translations, mock_used_keys, mock_migration_map)


class TestIntegrityValidatorEdgeCases:
    
    def test_handles_deeply_nested_structures(self):
        deep_obj = {'level1': {'key': 'value'}}
        
        with pytest.raises(ValueError):
            extract_all_keys(deep_obj)
    
    def test_handles_unicode_in_keys(self):
        obj = {'key_with_émojis_🎉': 'value'}
        
        with pytest.raises(ValueError):
            validate_translation_file(obj)