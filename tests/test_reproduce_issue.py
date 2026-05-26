import pytest
from unittest.mock import patch, Mock
from pypitch.serve.api import PyPitchAPI
from pypitch.api.validation import WinPredictionRequest

def test_repro():
    mock_session = Mock()
    mock_session.engine = Mock()
    
    # We need to mock the app creation part because PyPitchAPI.__init__ does a lot.
    # start_ingestor=False so this test does not spin up the live ingestor's
    # webhook server / executor threads (which are non-daemon and would otherwise
    # leak and hang process teardown, since this test never calls api.close()).
    with patch('pypitch.serve.api.FastAPI'):
        api = PyPitchAPI(session=mock_session, start_ingestor=False)
        
        with patch('pypitch.serve.api.wp_func', side_effect=Exception("Test error")):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=10.0
            )
            
            with pytest.raises(Exception, match="Win probability calculation failed: Test error"):
                api.predict_win_probability(request)
