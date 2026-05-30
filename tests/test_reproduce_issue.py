import pytest
from unittest.mock import patch, Mock
from midwicket.serve.api import MidwicketAPI
from midwicket.api.validation import WinPredictionRequest

def test_repro():
    mock_session = Mock()
    mock_session.engine = Mock()
    
    # We need to mock the app creation part because MidwicketAPI.__init__ does a lot
    with patch('midwicket.serve.api.FastAPI'):
        api = MidwicketAPI(session=mock_session, start_ingestor=False)
        
        with patch('midwicket.serve.api.wp_func', side_effect=Exception("Test error")):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=10.0
            )
            
            with pytest.raises(Exception, match="Win probability calculation failed: Test error"):
                api.predict_win_probability(request)


def test_sim_predict_win_success():
    from midwicket.api.sim import predict_win
    from midwicket.api.session import init
    import tempfile
    from pathlib import Path
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_sim.duckdb"
        session = init(str(db_path))
        
        # Populate the registry so venue is resolved
        from datetime import date
        session.registry.resolve_venue("Wankhede Stadium", match_date=date.today(), auto_ingest=True)
        
        # Let's mock the executor since it might raise NotImplementedError or return mock response
        with patch.object(session.executor, "execute") as mock_execute:
            mock_execute.return_value = Mock(data={"win_prob": 0.45})
            
            res = predict_win(
                venue="Wankhede Stadium",
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=10.0
            )
            assert res == {"win_prob": 0.45}
            mock_execute.assert_called_once()

