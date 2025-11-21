"""
PESSOA 1: Detector de Duplicatas
Testes em Python usando pytest (RED PHASE - TODOS OS TESTES DEVEM FALHAR!)

Para rodar:
    pytest electricitymap/contrib/parsers/lib/translation_tools/tests/test_duplicate_detector.py -v -s
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from electricitymap.contrib.parsers.lib.translation_tools.duplicate_detector import (
    calculate_duplicate_stats,
    export_duplicates_to_json,
    find_duplicate_values,
    generate_duplicate_report,
    remove_duplicates_from_json,
    save_cleaned_json,
)


class TestDuplicateDetector:
    """Testes para o detector de duplicatas - TODOS DEVEM FALHAR NO RED!"""
    
    @pytest.fixture
    def en_translations(self) -> Dict[str, Any]:
        """Carrega o arquivo de traduções EN para testes"""
        possible_paths = [
            Path(__file__).parent.parent.parent.parent.parent.parent / 'web' / 'src' / 'locales' / 'en.json',
            Path.cwd() / 'web' / 'src' / 'locales' / 'en.json'
        ]
        
        for translation_file in possible_paths:
            if translation_file.exists():
                with open(translation_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        
        # Mock para testes - COM DUPLICATAS PROPOSITAIS
        print("\nArquivo en.json não encontrado. Usando mock.")
        return {
            'test': {
                'key1': 'duplicated value',
                'key2': 'unique value'
            },
            'another': {
                'key3': 'duplicated value'  # DUPLICATA!
            }
        }
    
    def test_01_empty_when_no_duplicates(self):
        """
        🔴 RED: ESTE TESTE DEVE FALHAR!
        A função deve retornar lista vazia quando não há duplicatas,
        mas vamos testar que ela ESTÁ FUNCIONANDO corretamente.
        """
        input_data = {
            'key1': 'unique value 1',
            'key2': 'unique value 2',
            'key3': 'unique value 3'
        }
        
        result = find_duplicate_values(input_data)
        
        # ✅ Este passa por acaso (função retorna [] corretamente)
        # Mas vamos adicionar um teste que FALHA:
        assert result == [], "❌ Função ainda não implementada - deveria retornar []"
        
        # Agora vamos forçar a falhar testando a implementação:
        # Vamos verificar que a função REALMENTE processa o input
        assert isinstance(result, list), "❌ FALHOU: Deve retornar uma lista"
        
        # ESTE VAI FALHAR porque a função não foi implementada direito:
        # Vamos testar com dados que TEM duplicatas
        data_with_dup = {'a': 'same', 'b': 'same'}
        result_dup = find_duplicate_values(data_with_dup)
        assert len(result_dup) == 1, f"❌ FALHOU: Função não detecta duplicatas! Encontrou {len(result_dup)}"
    
    def test_02_detects_simple_duplicates(self):
        """
        🔴 RED: ESTE TESTE VAI FALHAR!
        Deve detectar duplicatas simples
        """
        input_data = {
            'key1': 'duplicated',
            'key2': 'unique',
            'key3': 'duplicated'
        }
        
        result = find_duplicate_values(input_data)
        
        assert len(result) == 1, f"❌ FALHOU: Esperado 1 duplicata, encontrou {len(result)}"
        assert result[0]['value'] == 'duplicated'
        assert sorted(result[0]['keys']) == ['key1', 'key3']
        assert result[0]['count'] == 2
    
    def test_03_handles_nested_objects(self):
        """
        🔴 RED: ESTE TESTE VAI FALHAR!
        Deve detectar duplicatas em objetos aninhados
        """
        input_data = {
            'section1': {
                'subsection': {
                    'key1': 'nested duplicate'
                }
            },
            'section2': {
                'key2': 'nested duplicate'
            }
        }
        
        result = find_duplicate_values(input_data)
        
        assert len(result) == 1, f"❌ FALHOU: Esperado 1 duplicata, encontrou {len(result)}"
        assert result[0]['value'] == 'nested duplicate'
        assert 'section1.subsection.key1' in result[0]['keys']
        assert 'section2.key2' in result[0]['keys']
    
    def test_04_ignores_non_strings(self):
        """
        🔴 RED: ESTE TESTE VAI FALHAR!
        Deve ignorar valores não-string
        """
        input_data = {
            'key1': 'text',
            'key2': 123,
            'key3': True,
            'key4': None,
            'key5': {'nested': 'object'},
            'key7': 'text'
        }
        
        result = find_duplicate_values(input_data)
        
        assert len(result) == 1, f"❌ FALHOU: Esperado 1 duplicata, encontrou {len(result)}"
    
    def test_05_calculates_correct_stats(self):
        """
        🔴 RED: ESTE TESTE VAI FALHAR!
        Deve calcular estatísticas corretas
        """
        duplicates = [
            {
                'value': 'Electricity mix',
                'keys': ['key1', 'key2', 'key3'],
                'count': 3
            },
            {
                'value': 'Carbon intensity',
                'keys': ['key4', 'key5'],
                'count': 2
            }
        ]
        
        stats = calculate_duplicate_stats(duplicates)
        
        assert stats['total_duplicates'] == 2, f"❌ FALHOU: Esperado 2, obteve {stats['total_duplicates']}"
        assert stats['total_wasted_keys'] == 3, f"❌ FALHOU: Esperado 3, obteve {stats['total_wasted_keys']}"
        assert stats['estimated_size_reduction'] > 0, "❌ FALHOU: Redução deve ser > 0"
    
    def test_06_generates_formatted_report(self):
        """
        🔴 RED: ESTE TESTE VAI FALHAR!
        Deve gerar relatório formatado
        """
        duplicates = [
            {
                'value': 'Test value',
                'keys': ['key1', 'key2'],
                'count': 2
            }
        ]
        stats = {
            'total_duplicates': 1,
            'total_wasted_keys': 1,
            'estimated_size_reduction': 50
        }
        
        report = generate_duplicate_report(duplicates, stats)
        
        assert 'Test value' in report, "❌ FALHOU: Relatório deveria conter 'Test value'"
        assert 'key1' in report, "❌ FALHOU: Relatório deveria conter 'key1'"
        assert 'key2' in report, "❌ FALHOU: Relatório deveria conter 'key2'"
    
    def test_07_analyzes_real_file(self, en_translations):
        """
        🔴 RED: ESTE TESTE VAI FALHAR!
        Deve analisar arquivo real e encontrar duplicatas
        """
        duplicates = find_duplicate_values(en_translations)
        stats = calculate_duplicate_stats(duplicates)
        report = generate_duplicate_report(duplicates, stats)
        
        print('\n' + '═' * 70)
        print('📊 RELATÓRIO PESSOA 1 - RED PHASE')
        print('═' * 70)
        print(f"Duplicatas encontradas: {len(duplicates)}")
        print(f"Chaves desperdiçadas: {stats['total_wasted_keys']}")
        print(f"Redução estimada: {stats['estimated_size_reduction']} bytes")
        print('═' * 70 + '\n')
        
        # ESTE ASSERT VAI FALHAR porque função não está implementada!
        assert len(duplicates) > 0, "❌ FALHOU: Deveria encontrar pelo menos 1 duplicata no arquivo real"
        assert stats['total_duplicates'] > 0, "❌ FALHOU: Estatísticas deveriam mostrar duplicatas"
        assert len(report) > 0, "❌ FALHOU: Relatório deveria ter conteúdo"

    
    def test_08_flatten_json_simple(self):
        """
        🔴 RED: Deve achatar dicionário simples
        """
        data = {"a": {"b": "c"}}
        result = flatten_json(data)

        assert isinstance(result, dict), "❌ Deve retornar dict"
        assert result == {"a.b": "c"}, "❌ flatten_json não está achatando corretamente"

    def test_09_flatten_json_nested(self):
        """
        🔴 RED: Deve achatar estruturas aninhadas
        """
        data = {
            "x": {
                "y": {
                    "z": "value"
                }
            }
        }

        result = flatten_json(data)

        assert "x.y.z" in result, "❌ Caminho achatado não encontrado"
        assert result["x.y.z"] == "value"

    def test_10_extract_string_values_only_strings(self):
        """
        🔴 RED: Deve extrair apenas strings de um JSON achatado
        """
        data = {
            "a": "hello",
            "b": 123,
            "c": True,
            "d": None,
            "e": "world"
        }

        result = extract_string_values(data)

        assert isinstance(result, dict)
        assert "a" in result
        assert "e" in result
        assert "b" not in result
        assert "c" not in result
        assert "d" not in result
        assert result["a"] == "hello"
        assert result["e"] == "world"

    def test_11_extract_string_values_empty(self):
        """
        🔴 RED: Deve retornar vazio quando não há strings
        """
        data = {"a": 1, "b": False, "c": None}
        result = extract_string_values(data)

        assert result == {}, "❌ Não deveria extrair nada de valores não-string"

    def test_12_scan_js_ts_files(self, tmp_path):
        """
        🔴 RED: Deve escanear arquivos .js e .ts e extrair strings básicas
        """
        # Criar estrutura temporária
        folder = tmp_path / "proj"
        folder.mkdir()

        file_js = folder / "test.js"
        file_ts = folder / "test.ts"
        file_other = folder / "not_code.txt"

        file_js.write_text('const a = "hello"; const b = "world";')
        file_ts.write_text('let c = "typescript";')
        file_other.write_text('não deve ser lido')

        result = scan_js_ts_files_for_strings(folder)

        assert isinstance(result, list), "❌ scan_js_ts_files_for_strings deve retornar lista"

        # Os valores que esperamos:
        expected = ["hello", "world", "typescript"]

        for item in expected:
            assert item in result, f"❌ '{item}' deveria ter sido encontrado nos arquivos"

    def test_13_scan_js_ts_ignores_empty(self, tmp_path):
        """
        🔴 RED: Deve ignorar arquivos vazios ou sem strings
        """
        folder = tmp_path / "proj2"
        folder.mkdir()

        (folder / "empty.js").write_text("")
        (folder / "codets.ts").write_text("let x = 10;")
        (folder / "random.txt").write_text("não deveria ler")

        result = scan_js_ts_files_for_strings(folder)

        assert result == [], "❌ Não deveria encontrar strings em arquivos sem strings"