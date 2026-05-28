import pytest
from unittest.mock import patch, MagicMock

import midwicket as md
from midwicket.api.models import MidwicketResultSet, MidwicketResultDict
from midwicket.explore import explore_player

def test_midwicket_result_set_html():
    data = [{"player": "A", "runs": 100}, {"player": "B", "runs": 50.555}]
    rs = MidwicketResultSet(data)
    html = rs._repr_html_()
    assert "player" in html
    assert "100" in html
    assert "50.56" in html # formatting

    # Test empty
    rs_empty = MidwicketResultSet([])
    assert "No results found" in rs_empty._repr_html_()

    # Test non-dict contents fallback
    rs_nondict = MidwicketResultSet([1, 2, 3])
    assert rs_nondict._repr_html_() == "[1, 2, 3]"

def test_midwicket_result_dict_html():
    data = {"player": "A", "runs": 100, "average": 50.555}
    rd = MidwicketResultDict(data)
    html = rd._repr_html_()
    assert "player" in html
    assert "100" in html
    assert "50.56" in html

    rd_empty = MidwicketResultDict({})
    assert "No data available" in rd_empty._repr_html_()

def test_midwicket_result_set_repr(monkeypatch):
    data = [{"player": "A", "runs": 100}]
    rs = MidwicketResultSet(data)
    # mock sys.stdout.isatty to True
    import sys
    monkeypatch.setattr(sys.stdout, 'isatty', lambda: True)
    # The __repr__ intercepts rich rendering, so it should return "" or fallback
    res = rs.__repr__()
    assert isinstance(res, str)

def test_midwicket_result_dict_repr(monkeypatch):
    data = {"player": "A", "runs": 100}
    rd = MidwicketResultDict(data)
    import sys
    monkeypatch.setattr(sys.stdout, 'isatty', lambda: True)
    res = rd.__repr__()
    assert isinstance(res, str)

@patch('builtins.print')
def test_explore_player_no_ipython(mock_print):
    # If get_ipython is missing, it should just print
    explore_player()
    mock_print.assert_called()

def test_explore_player_with_ipython(monkeypatch):
    import sys
    # mock ipython components
    mock_ipywidgets = MagicMock()
    mock_display = MagicMock()
    
    # We must patch sys.modules to mock ipywidgets inside the function import
    monkeypatch.setitem(sys.modules, 'ipywidgets', mock_ipywidgets)
    monkeypatch.setitem(sys.modules, 'IPython.display', mock_display)
    
    def fake_get_ipython():
        return True
    import builtins
    monkeypatch.setattr(builtins, 'get_ipython', fake_get_ipython, raising=False)
    
    # Mock session
    class MockEngine:
        def execute_sql(self, sql):
            class MockResult:
                def fetchall(self):
                    return [("A",), ("B",)]
            return MockResult()
            
    class MockSession:
        engine = MockEngine()
        
    monkeypatch.setattr("midwicket.api.session.MidwicketSession.get", lambda: MockSession())
    
    explore_player()
    assert mock_ipywidgets.HTML.called


from unittest.mock import patch
import sys
from midwicket.cli import main

def test_cli_version(capsys):
    with patch.object(sys, 'argv', ['midwicket', '--version']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        captured = capsys.readouterr()
        assert "Midwicket v" in captured.out

def test_cli_help(capsys):
    with patch.object(sys, 'argv', ['midwicket', '--help']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        captured = capsys.readouterr()
        assert "Usage: midwicket" in captured.out
