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
