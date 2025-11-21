import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# ============================================================
# CONSTANTES (melhor organização)
# ============================================================

DEFAULT_ENCODING = 'utf-8'
BYTES_TO_KB = 1024
MIN_STRING_LENGTH = 1  # Ignorar strings muito curtas


# ============================================================
# DATA CLASSES (melhor tipagem)
# ============================================================

@dataclass
class DuplicateEntry:
    """
    Representa uma entrada de valor duplicado.
    
    Attributes:
        value: O valor de texto duplicado
        keys: Lista de chaves que compartilham este valor
        count: Número de ocorrências
    """
    value: str
    keys: List[str]
    count: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário (compatibilidade com testes)"""
        return {
            'value': self.value,
            'keys': self.keys,
            'count': self.count
        }


@dataclass
class DuplicateStats:
    """
    Estatísticas sobre duplicatas encontradas.
    
    Attributes:
        total_duplicates: Número de valores duplicados únicos
        total_wasted_keys: Número total de chaves que podem ser removidas
        estimated_size_reduction: Economia de espaço estimada em bytes
    """
    total_duplicates: int
    total_wasted_keys: int
    estimated_size_reduction: int
    
    def to_dict(self) -> Dict[str, int]:
        """Converte para dicionário (compatibilidade com testes)"""
        return {
            'total_duplicates': self.total_duplicates,
            'total_wasted_keys': self.total_wasted_keys,
            'estimated_size_reduction': self.estimated_size_reduction
        }
    
    @property
    def size_in_kb(self) -> float:
        """Retorna tamanho em KB"""
        return self.estimated_size_reduction / BYTES_TO_KB


# ============================================================
# FUNÇÕES AUXILIARES (melhor separação de responsabilidades)
# ============================================================

def _is_valid_string_value(value: Any) -> bool:
    """
    Verifica se um valor é uma string válida para análise.
    
    Args:
        value: Valor a ser verificado
        
    Returns:
        True se for string não-vazia
    """
    return (
        isinstance(value, str) and 
        value.strip() != '' and 
        len(value) >= MIN_STRING_LENGTH
    )


def _build_path(current_path: str, key: str) -> str:
    """
    Constrói o caminho completo de uma chave.
    
    Args:
        current_path: Caminho atual
        key: Nova chave a adicionar
        
    Returns:
        Caminho completo (ex: 'section.subsection.key')
    """
    return f"{current_path}.{key}" if current_path else key


def _calculate_string_size_bytes(text: str) -> int:
    """
    Calcula tamanho de string em bytes (UTF-8).
    
    Args:
        text: String a medir
        
    Returns:
        Tamanho em bytes
    """
    return len(text.encode(DEFAULT_ENCODING))


# ============================================================
# FUNÇÕES PRINCIPAIS (refatoradas)
# ============================================================

def find_duplicate_values(
    obj: Dict[str, Any],
    prefix: str = ''
) -> List[Dict[str, Any]]:
    """
    🔵 REFATORADO: Versão otimizada com melhor performance!
    
    Encontra todos os valores duplicados em um objeto de tradução.
    Utiliza defaultdict para melhor performance.
    
    Args:
        obj: Objeto de tradução (pode ser aninhado)
        prefix: Prefixo para chaves aninhadas (usado internamente)
        
    Returns:
        Lista de dicionários com duplicatas ordenadas por ocorrências
        
    Complexidade:
        - Tempo: O(n) onde n = número total de chaves
        - Espaço: O(m) onde m = número de valores únicos
        
    Exemplo:
        >>> data = {
        ...     'section1': {'key1': 'duplicated'},
        ...     'section2': {'key2': 'duplicated'}
        ... }
        >>> result = find_duplicate_values(data)
        >>> len(result)
        1
        >>> result[0]['count']
        2
    """
    # Usar defaultdict para melhor performance
    value_map: Dict[str, List[str]] = defaultdict(list)
    
    def traverse(current: Any, current_path: str) -> None:
        """
        Travessia recursiva otimizada.
        
        Args:
            current: Valor atual
            current_path: Caminho da chave
        """
        if _is_valid_string_value(current):
            # String válida - adicionar ao mapa
            value_map[current].append(current_path)
            
        elif isinstance(current, dict):
            # Objeto - percorrer recursivamente
            for key, value in current.items():
                new_path = _build_path(current_path, key)
                traverse(value, new_path)
        
        # Ignorar outros tipos automaticamente
    
    # Iniciar travessia
    traverse(obj, prefix)
    
    # Filtrar e criar objetos DuplicateEntry
    duplicates: List[DuplicateEntry] = []
    
    for value, keys in value_map.items():
        if len(keys) > 1:
            entry = DuplicateEntry(
                value=value,
                keys=sorted(keys),  # Ordenar para consistência
                count=len(keys)
            )
            duplicates.append(entry)
    
    # Ordenar por contagem (maior primeiro) e depois alfabeticamente
    duplicates.sort(key=lambda x: (-x.count, x.value))
    
    # Converter para dict para compatibilidade com testes
    return [dup.to_dict() for dup in duplicates]


def calculate_duplicate_stats(duplicates: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    🔵 REFATORADO: Versão com melhor clareza e validação!
    
    Calcula estatísticas sobre duplicatas com validação de entrada.
    
    Args:
        duplicates: Lista de duplicatas
        
    Returns:
        Dicionário com estatísticas
        
    Raises:
        ValueError: Se duplicates não for uma lista válida
        
    Exemplo:
        >>> dups = [
        ...     {'value': 'test', 'keys': ['k1', 'k2', 'k3'], 'count': 3}
        ... ]
        >>> stats = calculate_duplicate_stats(dups)
        >>> stats['total_duplicates']
        1
        >>> stats['total_wasted_keys']
        2
    """
    # Validação de entrada
    if not isinstance(duplicates, list):
        raise ValueError("duplicates deve ser uma lista")
    
    if len(duplicates) == 0:
        return DuplicateStats(
            total_duplicates=0,
            total_wasted_keys=0,
            estimated_size_reduction=0
        ).to_dict()
    
    total_duplicates = len(duplicates)
    
    # Calcular chaves desperdiçadas
    # Para cada duplicata, (count - 1) chaves podem ser removidas
    total_wasted_keys = sum(
        max(0, dup['count'] - 1)  # Garantir não-negativo
        for dup in duplicates
    )
    
    # Estimar redução de tamanho
    # Cada chave extra economiza o tamanho do valor
    estimated_size_reduction = sum(
        _calculate_string_size_bytes(dup['value']) * max(0, dup['count'] - 1)
        for dup in duplicates
    )
    
    stats = DuplicateStats(
        total_duplicates=total_duplicates,
        total_wasted_keys=total_wasted_keys,
        estimated_size_reduction=estimated_size_reduction
    )
    
    return stats.to_dict()


def generate_duplicate_report(
    duplicates: List[Dict[str, Any]],
    stats: Dict[str, int],
    max_items: Optional[int] = None,
    show_keys: bool = True
) -> str:
    """
    🔵 REFATORADO: Versão com mais opções de formatação!
    
    Gera relatório formatado com opções de customização.
    
    Args:
        duplicates: Lista de duplicatas
        stats: Estatísticas
        max_items: Número máximo de itens a exibir (None = todos)
        show_keys: Se deve exibir todas as chaves
        
    Returns:
        Relatório formatado
        
    Exemplo:
        >>> dups = [{'value': 'Test', 'keys': ['k1', 'k2'], 'count': 2}]
        >>> stats = {'total_duplicates': 1, 'total_wasted_keys': 1, 'estimated_size_reduction': 4}
        >>> report = generate_duplicate_report(dups, stats, max_items=5)
        >>> 'Test' in report
        True
    """
    # Caso especial: sem duplicatas
    if len(duplicates) == 0:
        return '✅ Nenhuma duplicata encontrada! O arquivo está otimizado.'
    
    # Cabeçalho
    lines = [
        '📊 ANÁLISE DE DUPLICATAS',
        '',
        f"Total de valores duplicados: {stats['total_duplicates']}",
        f"Total de chaves desperdiçadas: {stats['total_wasted_keys']}",
        f"Redução estimada: {stats['estimated_size_reduction']} bytes (~{stats['estimated_size_reduction']/BYTES_TO_KB:.2f} KB)",
        '',
        '─' * 70,
        ''
    ]
    
    # Limitar itens se necessário
    items_to_show = duplicates[:max_items] if max_items else duplicates
    
    # Listar duplicatas
    for index, dup in enumerate(items_to_show, 1):
        lines.append(f"{index}. \"{dup['value']}\"")
        lines.append(f"   💾 Usado em {dup['count']} lugares:")
        
        if show_keys:
            for key in dup['keys']:
                lines.append(f"      - {key}")
        else:
            lines.append(f"      (chaves ocultas)")
        
        lines.append('')
    
    # Indicar se há mais itens
    if max_items and len(duplicates) > max_items:
        remaining = len(duplicates) - max_items
        lines.append(f"... e mais {remaining} duplicatas não exibidas.")
        lines.append('')
    
    return '\n'.join(lines)


def export_duplicates_to_json(
    duplicates: List[Dict[str, Any]],
    output_path: str,
    include_metadata: bool = True
) -> None:
    """
    🔵 REFATORADO: Versão com melhor tratamento de erros!
    
    Exporta duplicatas para JSON com validação e metadata.
    
    Args:
        duplicates: Lista de duplicatas
        output_path: Caminho do arquivo de saída
        include_metadata: Se deve incluir metadata no JSON
        
    Raises:
        IOError: Se não conseguir escrever o arquivo
        
    Exemplo:
        >>> dups = [{'value': 'Test', 'keys': ['k1'], 'count': 1}]
        >>> export_duplicates_to_json(dups, '/tmp/test.json')
        ✅ Relatório exportado para: /tmp/test.json
    """
    # Preparar dados
    data: Dict[str, Any] = {}
    
    if include_metadata:
        data['generated_at'] = datetime.utcnow().isoformat() + 'Z'
        data['generated_by'] = 'Pessoa 1 - Detector de Duplicatas (Python)'
        data['author'] = 'Kaleb Macedo'
        data['version'] = '2.0 (Refatorada)'
        data['total_duplicates'] = len(duplicates)
    
    data['duplicates'] = duplicates
    
    # Criar diretórios se necessário
    output_file = Path(output_path)
    
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Salvar JSON
        with open(output_file, 'w', encoding=DEFAULT_ENCODING) as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Relatório exportado para: {output_path}")
        
    except IOError as e:
        print(f"❌ Erro ao exportar relatório: {e}")
        raise


# ============================================================
# FUNÇÕES AUXILIARES DE CONVENIÊNCIA
# ============================================================

def remove_duplicates_from_json(data: Dict[str, Any], duplicates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Remove duplicatas do objeto de tradução mantendo apenas a primeira ocorrência.
    """
    def delete_path(obj: Dict[str, Any], path: str) -> None:
        """
        Deleta uma chave dado um path tipo "a.b.c".
        """
        parts = path.split('.')
        *parents, last = parts
        curr = obj
        for p in parents:
            if p not in curr or not isinstance(curr[p], dict):
                return  # caminho inválido → ignora
            curr = curr[p]
        curr.pop(last, None)

    # Clonar para não alterar o original
    cleaned = json.loads(json.dumps(data))

    for dup in duplicates:
        keys = dup['keys']
        keys_to_remove = keys[1:]  # remover do segundo em diante

        for key in keys_to_remove:
            delete_path(cleaned, key)

    return cleaned


def save_cleaned_json(cleaned: Dict[str, Any], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)


def analyze_translation_file(file_path: str) -> tuple[List[Dict], Dict[str, int], str]:
    """
    🔵 NOVA: Função de conveniência para análise completa!
    
    Analisa um arquivo de tradução completo.
    
    Args:
        file_path: Caminho do arquivo JSON
        
    Returns:
        Tupla (duplicatas, estatísticas, relatório)
        
    Raises:
        FileNotFoundError: Se arquivo não existe
        json.JSONDecodeError: Se JSON inválido
    """
    with open(file_path, 'r', encoding=DEFAULT_ENCODING) as f:
        data = json.load(f)
    
    duplicates = find_duplicate_values(data)
    stats = calculate_duplicate_stats(duplicates)
    report = generate_duplicate_report(duplicates, stats)
    
    return duplicates, stats, report


def get_top_duplicates(duplicates: List[Dict[str, Any]], n: int = 10) -> List[Dict[str, Any]]:
    """
    🔵 NOVA: Retorna as N duplicatas mais frequentes!
    
    Args:
        duplicates: Lista de duplicatas
        n: Número de itens a retornar
        
    Returns:
        Top N duplicatas
    """
    return duplicates[:n]


# ============================================================
# MAIN (refatorado)
# ============================================================

def main():
    """
    Função principal otimizada com melhor UX.
    
    Uso:
        python -m electricitymap.contrib.parsers.lib.translation_tools.duplicate_detector
    """
    print('\n' + '═' * 70)
    print('🔍 PESSOA 1 - DETECTOR DE DUPLICATAS (REFATORADO)')
    print('Autor: Kaleb Macedo')
    print('Data: 2025-11-21')
    print('Versão: 2.0 (Refatorada)')
    print('═' * 70 + '\n')
    
    # Tentar encontrar arquivo en.json
    possible_paths = [
        Path(__file__).parent.parent.parent.parent / 'web' / 'src' / 'locales' / 'en.json',
        Path.cwd() / 'web' / 'src' / 'locales' / 'en.json'
    ]
    
    en_file = None
    for path in possible_paths:
        if path.exists():
            en_file = path
            break
    
    if not en_file:
        print("❌ Arquivo en.json não encontrado!")
        print("   Procurado em:")
        for path in possible_paths:
            print(f"   - {path}")
        return
    
    print(f"📁 Carregando: {en_file}")
    
    try:
        # Usar função de conveniência
        duplicates, stats, report = analyze_translation_file(str(en_file))
        
        print(f"✅ Arquivo carregado e analisado!\n")
        
        # Exibir relatório (limitado aos top 10)
        print('═' * 70)
        print('📊 RELATÓRIO DE DUPLICATAS (TOP 10)')
        print('═' * 70)
        
        top_10_report = generate_duplicate_report(
            get_top_duplicates(duplicates, 10),
            stats,
            show_keys=True
        )
        print(top_10_report)
        
        print('═' * 70 + '\n')
        
        # Exportar JSON completo
        output_dir = Path.cwd() / 'consolidation-reports'
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / '1-duplicates.json'
        
        export_duplicates_to_json(duplicates, str(output_file))
        
        # Resumo final
        print('\n' + '═' * 70)
        print('📈 RESUMO EXECUTIVO')
        print('═' * 70)
        print(f"✅ Duplicatas encontradas: {stats['total_duplicates']}")
        print(f"✅ Chaves removíveis: {stats['total_wasted_keys']}")
        print(f"✅ Economia estimada: {stats['estimated_size_reduction']} bytes (~{stats['estimated_size_reduction']/BYTES_TO_KB:.2f} KB)")
        print(f"✅ Relatório completo: {output_file}")
        print('═' * 70 + '\n')
        
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo não encontrado!")
    except json.JSONDecodeError as e:
        print(f"❌ Erro ao ler JSON: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")


if __name__ == '__main__':
    main()