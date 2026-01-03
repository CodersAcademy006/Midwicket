import pytest
from unittest.mock import patch, Mock
from pypitch.serve.api import PyPitchAPI
from pypitch.api.validation import WinPredictionRequest

TEST_ERROR_MSG = "Test error"

def test_predict_win_probability_wraps_internal_errors():
    """Verify predict_win_probability wraps internal exceptions with a descriptive message."""
    mock_session = Mock()
    mock_session.engine = Mock()
    
    # We need to mock the app creation part because PyPitchAPI.__init__ does a lot
    with patch('pypitch.serve.api.FastAPI'):
        api = PyPitchAPI(session=mock_session)
        
        with patch('pypitch.serve.api.wp_func', side_effect=Exception(TEST_ERROR_MSG)):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=10.0
            )
            
            with pytest.raises(Exception, match=f"Win probability calculation failed: {TEST_ERROR_MSG}"):
                api.predict_win_probability(request)
