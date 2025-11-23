"""
PESSOA 3: Gerador de Mapeamento
Testes em Python usando pytest
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from ..mapping_generator import (
    ConsolidationStrategy,
    MigrationMap,
    choose_canonical_key,
    create_migration_map,
    export_strategies_to_json,
    generate_consolidation_strategy,
    generate_migration_script,
)


class TestMappingGenerator:
    """Testes para o gerador de mapeamento"""

    def test_choose_canonical_key_selects_most_used(self):
        """Deve escolher a chave mais usada"""
        keys = ["key.a", "key.b", "key.c"]
        usage_counts = {
            "key.a": 5,
            "key.b": 10,  # Mais usada
            "key.c": 3,
        }

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical == "key.b"

    def test_choose_canonical_key_prefers_shorter_on_tie(self):
        """Deve escolher a chave mais curta em caso de empate"""
        keys = ["long.key.name", "short", "medium.key"]
        usage_counts = {"long.key.name": 5, "short": 5, "medium.key": 5}

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical == "short"

    def test_choose_canonical_key_prefers_no_numbers(self):
        """Deve preferir chaves que não têm números"""
        keys = ["section.item", "section.item24h", "section.item2"]
        usage_counts = {"section.item": 1, "section.item24h": 10, "section.item2": 10}

        canonical = choose_canonical_key(keys, usage_counts)

        # Deve preferir 'section.item' por ser mais genérica
        assert canonical == "section.item"

    def test_choose_canonical_key_alphabetical_on_full_tie(self):
        """Deve escolher primeira chave alfabeticamente se todos critérios empatarem"""
        keys = ["zebra", "apple", "banana"]
        usage_counts = {"zebra": 0, "apple": 0, "banana": 0}

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical == "apple"

    def test_choose_canonical_key_handles_single_key(self):
        """Deve lidar com lista de uma única chave"""
        keys = ["only.key"]
        usage_counts = {"only.key": 5}

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical == "only.key"

    def test_choose_canonical_key_handles_empty_list(self):
        """Deve lidar com lista vazia"""
        keys = []
        usage_counts = {}

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical == ""

    def test_generate_consolidation_strategy_complete(self):
        """Deve gerar estratégia completa"""
        value = "Electricity mix"
        keys = [
            "country-history.electricityorigin.24h",
            "country-history.electricityproduction.24h",
            "country-history.electricityorigin.72h",
        ]
        usage_counts = {
            "country-history.electricityorigin.24h": 8,
            "country-history.electricityproduction.24h": 12,
            "country-history.electricityorigin.72h": 5,
        }

        strategy = generate_consolidation_strategy(value, keys, usage_counts)

        assert strategy["canonical_key"] == "country-history.electricityproduction.24h"
        assert len(strategy["deprecated_keys"]) == 2
        assert strategy["value"] == "Electricity mix"
        assert strategy["reason"] != ""
        assert isinstance(strategy["reason"], str)

    def test_generate_consolidation_strategy_includes_clear_reason(self):
        """Deve incluir razão clara da escolha"""
        value = "Test"
        keys = ["key1", "key2"]
        usage_counts = {"key1": 10, "key2": 2}

        strategy = generate_consolidation_strategy(value, keys, usage_counts)

        assert "10" in strategy["reason"]
        assert "usage" in strategy["reason"].lower()

    def test_create_migration_map_correct_mapping(self):
        """Deve criar mapeamento correto de old -> new"""
        strategies = [
            {
                "canonical_key": "keep.this",
                "deprecated_keys": ["remove.this", "remove.that"],
                "value": "Value",
                "reason": "Most used",
            }
        ]

        migration_map = create_migration_map(strategies)

        assert migration_map["remove.this"] == "keep.this"
        assert migration_map["remove.that"] == "keep.this"
        assert "keep.this" not in migration_map  # Chave canônica não está no mapa

    def test_create_migration_map_handles_multiple_strategies(self):
        """Deve lidar com múltiplas estratégias"""
        strategies = [
            {
                "canonical_key": "canonical1",
                "deprecated_keys": ["old1", "old2"],
                "value": "Value1",
                "reason": "Reason1",
            },
            {
                "canonical_key": "canonical2",
                "deprecated_keys": ["old3", "old4"],
                "value": "Value2",
                "reason": "Reason2",
            },
        ]

        migration_map = create_migration_map(strategies)

        assert len(migration_map) == 4
        assert migration_map["old1"] == "canonical1"
        assert migration_map["old3"] == "canonical2"

    def test_create_migration_map_handles_empty_strategies(self):
        """Deve lidar com lista vazia de estratégias"""
        migration_map = create_migration_map([])

        assert migration_map == {}
        assert isinstance(migration_map, dict)

    def test_generate_migration_script_creates_valid_python(self):
        """Deve gerar script Python válido"""
        migration_map = {"old.key.1": "new.key.1", "old.key.2": "new.key.2"}

        script = generate_migration_script(migration_map)

        assert "old.key.1" in script
        assert "new.key.1" in script
        assert "def " in script  # Deve conter definições de função
        assert "TRANSLATION_KEY_MIGRATION_MAP" in script

    def test_generate_migration_script_includes_comments(self):
        """Deve incluir comentários explicativos"""
        migration_map = {"old": "new"}

        script = generate_migration_script(migration_map)

        assert "#" in script
        assert "Generated" in script or "Auto-generated" in script

    def test_generate_migration_script_is_valid_python_syntax(self):
        """Deve ser código Python válido sintaticamente"""
        migration_map = {"test.key": "test.key2"}

        script = generate_migration_script(migration_map)

        # Tentar compilar para verificar sintaxe
        try:
            compile(script, "<string>", "exec")
            syntax_valid = True
        except SyntaxError:
            syntax_valid = False

        assert syntax_valid

    def test_generate_migration_script_contains_helper_functions(self):
        """Deve conter funções auxiliares úteis"""
        migration_map = {"old": "new"}

        script = generate_migration_script(migration_map)

        assert "migrate_key" in script
        assert "get_deprecated_keys" in script
        assert "is_deprecated_key" in script

    def test_export_strategies_to_json_creates_file(self, tmp_path):
        """Deve exportar estratégias para JSON"""
        strategies = [
            {
                "canonical_key": "key1",
                "deprecated_keys": ["key2"],
                "value": "Value",
                "reason": "Test",
            }
        ]
        migration_map = {"key2": "key1"}
        output_file = tmp_path / "strategies.json"

        export_strategies_to_json(strategies, migration_map, str(output_file))

        assert output_file.exists()

        with open(output_file, "r") as f:
            data = json.load(f)
            assert "strategies" in data
            assert "migration_map" in data
            assert "generated_by" in data
            assert "Pessoa 3" in data["generated_by"]


class TestMappingGeneratorIntegration:
    """Testes de integração com dados das outras pessoas"""

    def test_integration_with_person1_and_person2_data(self):
        """Deve processar duplicatas da Pessoa 1 e usos da Pessoa 2"""
        # Mock de dados que viriam das Pessoas 1 e 2
        duplicates_from_person1 = [
            {
                "value": "Electricity mix",
                "keys": [
                    "country-history.electricityorigin.24h",
                    "country-history.electricityproduction.24h",
                ],
                "count": 2,
            }
        ]

        usage_counts_from_person2 = {
            "country-history.electricityorigin.24h": 8,
            "country-history.electricityproduction.24h": 12,
        }

        strategies = []
        for dup in duplicates_from_person1:
            strategy = generate_consolidation_strategy(
                dup["value"], dup["keys"], usage_counts_from_person2
            )
            strategies.append(strategy)

        migration_map = create_migration_map(strategies)
        script = generate_migration_script(migration_map)

        print("\n" + "═" * 70)
        print("📊 RELATÓRIO PESSOA 3 - ESTRATÉGIA DE CONSOLIDAÇÃO (PYTHON)")
        print("═" * 70)
        print("\nEstratégias geradas:")
        for i, s in enumerate(strategies, 1):
            print(f"\n{i}. {s['value']}")
            print(f"   Manter: {s['canonical_key']}")
            print(f"   Remover: {', '.join(s['deprecated_keys'])}")
            print(f"   Razão: {s['reason']}")
        print(f"\nMapa de migração: {migration_map}")
        print("═" * 70 + "\n")

        assert len(strategies) == 1
        assert migration_map is not None
        assert script != ""


class TestMappingGeneratorEdgeCases:
    """Testes de casos extremos"""

    def test_handles_keys_with_special_characters(self):
        """Deve lidar com chaves com caracteres especiais"""
        keys = ["key-with-dash", "key.with.dots", "key_with_underscore"]
        usage_counts = {
            "key-with-dash": 5,
            "key.with.dots": 5,
            "key_with_underscore": 5,
        }

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical in keys

    def test_handles_very_long_key_names(self):
        """Deve lidar com nomes de chaves muito longos"""
        long_key = "very." * 50 + "long.key"
        keys = [long_key, "short"]
        usage_counts = {long_key: 10, "short": 10}

        canonical = choose_canonical_key(keys, usage_counts)

        assert canonical == "short"  # Deve preferir a mais curta
