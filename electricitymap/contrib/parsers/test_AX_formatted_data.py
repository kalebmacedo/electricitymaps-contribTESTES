# tests/parsers/test_AX_formatted_data.py
import types
from datetime import datetime
import pytest

# Ajuste o import abaixo se o caminho do módulo for diferente no seu fork
import electricitymap.contrib.parsers.AX as AX
from electricitymap.contrib.parsers.lib.exceptions import ParserException


# --------------------------------------------------------------------------------------
# Helpers de monkeypatch
# --------------------------------------------------------------------------------------

class _FixedDateTime:
    """Proxy para datetime.now(...) que devolve sempre o 'agora' fixo (timezone-aware)."""
    _now = None

    @classmethod
    def now(cls, tz=None):
        # Ignoramos tz aqui porque já entregamos o _now com tzinfo correto.
        return cls._now


def _set_fixed_now(monkeypatch, aware_dt):
    """
    Substitui AX.datetime.now por uma versão fixa que retorna 'aware_dt'.
    Mantemos apenas a interface necessária (atributo .now).
    """
    _FixedDateTime._now = aware_dt
    monkeypatch.setattr(
        AX, "datetime", types.SimpleNamespace(now=_FixedDateTime.now),
        raising=True
    )


def _stub_fetch(monkeypatch, data_list):
    """Substitui fetch_data para retornar exatamente o data_list informado."""
    def _fake_fetch(session, logger):
        return data_list
    monkeypatch.setattr(AX, "fetch_data", _fake_fetch, raising=True)


def _stub_output_lists(monkeypatch):
    """
    Espiona as listas de saída (ProductionBreakdownList, TotalConsumptionList, ExchangeList)
    para verificarmos appends e retornos sem depender das implementações reais.
    """
    calls = {"production": [], "consumption": [], "exchange": []}

    class FakeProductionList:
        def __init__(self, logger):
            pass
        def append(self, *, datetime, production, source, zoneKey):
            calls["production"].append(
                {"datetime": datetime, "production": production, "source": source, "zoneKey": zoneKey}
            )
        def to_list(self):
            return calls["production"]

    class FakeConsumptionList:
        def __init__(self, logger):
            pass
        def append(self, *, datetime, consumption, source, zoneKey):
            calls["consumption"].append(
                {"datetime": datetime, "consumption": consumption, "source": source, "zoneKey": zoneKey}
            )
        def to_list(self):
            return calls["consumption"]

    class FakeExchangeList:
        def __init__(self, logger):
            pass
        def append(self, *, datetime, netFlow, source, zoneKey):
            calls["exchange"].append(
                {"datetime": datetime, "netFlow": netFlow, "source": source, "zoneKey": zoneKey}
            )
        def to_list(self):
            return calls["exchange"]

    monkeypatch.setattr(AX, "ProductionBreakdownList", FakeProductionList, raising=True)
    monkeypatch.setattr(AX, "TotalConsumptionList",   FakeConsumptionList,  raising=True)
    monkeypatch.setattr(AX, "ExchangeList",           FakeExchangeList,     raising=True)
    return calls


# --------------------------------------------------------------------------------------
# Fixtures comuns
# --------------------------------------------------------------------------------------

@pytest.fixture
def fixed_zone_now(monkeypatch):
    """
    Fixa o 'agora' para 2025-10-28 12:34:00 no fuso AX.TIME_ZONE (zoneinfo).
    IMPORTANTE: zoneinfo não tem .localize(); criamos o datetime já com tzinfo.
    """
    aware_now = datetime(2025, 10, 28, 12, 34, 0, tzinfo=AX.TIME_ZONE)
    _set_fixed_now(monkeypatch, aware_now)
    return aware_now


@pytest.fixture
def spies(monkeypatch):
    return _stub_output_lists(monkeypatch)


# --------------------------------------------------------------------------------------
# CT1 — Processamento de Produção Válido
# CD1=F (date <= now), CD2=V, CD3=V  -> 1 append em produção
# --------------------------------------------------------------------------------------
def test_processamento_producao_valido(monkeypatch, fixed_zone_now, spies):
    # time base = 12:00 (<= 12:34) -> date <= now (CD1=F)
    data_list = [{
        "time": "12:00",
        "wind": 10.0,
        "fossil": 5.0,
        "consumption": 0.0,
        "sweden": 0.0,
        "alink": 0.0,
        "gustavs": 0.0,
    }]
    _stub_fetch(monkeypatch, data_list)

    zone_key = AX.ZoneKey("AX")
    result = AX.formatted_data(
        zone_key=zone_key,
        zone_key1=None,
        zone_key2=None,
        session=None,
        logger=None,
        data_type="production",
    )

    assert isinstance(result, list)
    assert len(spies["production"]) == 1, "Deveria registrar 1 entrada de produção"
    item = spies["production"][0]
    # corrected_date = 12:00 (index 0 => sem subtração de 15 min)
    assert item["datetime"].hour == 12 and item["datetime"].minute == 0
    assert item["zoneKey"] == zone_key
    assert item["production"].wind == 10.0
    assert item["production"].oil == 5.0


# --------------------------------------------------------------------------------------
# CT2 — Zone Key Nulo Durante Produção
# CD1=V (date > now) CD2=V CD3=F  -> deve lançar ParserException e não fazer append
# --------------------------------------------------------------------------------------
def test_zone_key_nulo_durante_producao(monkeypatch, fixed_zone_now, spies):
    # Para forçar CD1=V: time futuro 13:00 (> 12:34) -> método ajusta -1 dia internamente
    data_list = [{
        "time": "13:00",
        "wind": 8.0,
        "fossil": 2.0,
        "consumption": 0.0,
        "sweden": 0.0,
        "alink": 0.0,
        "gustavs": 0.0,
    }]
    _stub_fetch(monkeypatch, data_list)

    with pytest.raises(ParserException):
        AX.formatted_data(
            zone_key=None,              # CD3=F
            zone_key1=None,
            zone_key2=None,
            session=None,
            logger=None,
            data_type="production",     # CD2=V
        )

    # Garantia adicional: nada foi registrado em produção
    assert len(spies["production"]) == 0


# --------------------------------------------------------------------------------------
# CT3 — Tipo de Dado Inválido (Consumo) para o fluxo de produção
# CD1=F CD2=F ('consumption') CD3=V -> não tocar produção; consumo recebe 1
# --------------------------------------------------------------------------------------
def test_tipo_dado_invalido_consumo_para_producao(monkeypatch, fixed_zone_now, spies):
    data_list = [{
        "time": "12:15",            # <= 12:34 -> CD1=F
        "wind": 3.0,
        "fossil": 1.0,
        "consumption": 42.5,
        "sweden": 0.0,
        "alink": 0.0,
        "gustavs": 0.0,
    }]
    _stub_fetch(monkeypatch, data_list)

    result = AX.formatted_data(
        zone_key=AX.ZoneKey("AX"),  # CD3=V
        zone_key1=None,
        zone_key2=None,
        session=None,
        logger=None,
        data_type="consumption",    # CD2=F (não é 'production')
    )

    assert isinstance(result, list)
    assert len(spies["production"]) == 0, "Não deveria registrar produção quando data_type != 'production'"
    assert len(spies["consumption"]) == 1, "Deveria registrar 1 entrada de consumo"
    assert spies["consumption"][0]["consumption"] == 42.5
