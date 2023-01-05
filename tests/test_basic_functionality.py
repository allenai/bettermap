import bettermap


def f(x: float) -> float:
    return x * x


_INPUT = list(range(100))
_EXPECTED = list(map(f, _INPUT))


def test_map_per_process():
    result = list(bettermap.map_per_process(f, _INPUT))
    result.sort()
    assert result == _EXPECTED


def test_ordered_map_per_process():
    result = list(bettermap.ordered_map_per_process(f, _INPUT))
    assert result == _EXPECTED


def test_ordered_map_per_thread():
    result = list(bettermap.ordered_map_per_thread(f, _INPUT))
    assert result == _EXPECTED


def test_map_in_chunks():
    result = list(bettermap.map_in_chunks(f, _INPUT))
    result.sort()
    assert result == _EXPECTED


def test_ordered_map_in_chunks():
    result = list(bettermap.ordered_map_in_chunks(f, _INPUT))
    assert result == _EXPECTED
