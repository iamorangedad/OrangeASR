import pytest
import json
from unittest.mock import patch, MagicMock
from src.refiner import ASRRefiner


class TestASRRefiner:

    @pytest.fixture
    def refiner(self):
        return ASRRefiner(model_name="qwen3:4b", base_url="http://10.0.0.55:11434")

    @patch("src.refiner.requests.post")
    def test_refine_with_correction(self, mock_post, refiner):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "response": json.dumps({
                "audio_id": "001",
                "original_text": "direction to House Place 2900",
                "corrected_text": "direction to Hearth Place 2900",
            })
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        raw_input = [
            {"audio_id": "001", "transcribe_text": "direction to House Place 2900"},
            {"audio_id": "002", "transcribe_text": "No, not house, it is Hearth, H E A R T H."},
        ]

        result = refiner.refine(raw_input)

        assert isinstance(result, dict)
        assert result["audio_id"] == "001"
        assert "original_text" in result
        assert "corrected_text" in result
        assert "House" in result["original_text"]
        assert "Hearth" in result["corrected_text"]
        assert "2900" in result["corrected_text"]

    @patch("src.refiner.requests.post")
    def test_refine_single_segment(self, mock_post, refiner):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "response": json.dumps({
                "audio_id": "005",
                "original_text": "Hello world",
                "corrected_text": "Hello world",
            })
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        raw_input = [{"audio_id": "005", "transcribe_text": "Hello world"}]
        result = refiner.refine(raw_input)

        assert result["audio_id"] == "005"
        assert result["corrected_text"] == "Hello world"

    @patch("src.refiner.requests.post")
    def test_refine_handles_thinking_field(self, mock_post, refiner):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "thinking": json.dumps({
                "audio_id": "010",
                "original_text": "call mom",
                "corrected_text": "call Tom",
            })
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        raw_input = [
            {"audio_id": "010", "transcribe_text": "call mom"},
            {"audio_id": "011", "transcribe_text": "No, Tom, T-O-M"},
        ]

        result = refiner.refine(raw_input)

        assert result["audio_id"] == "010"
        assert result["corrected_text"] == "call Tom"

    @patch("src.refiner.requests.post")
    def test_refine_raises_on_connection_error(self, mock_post, refiner):
        import requests as req
        mock_post.side_effect = req.exceptions.ConnectionError("Connection refused")

        raw_input = [{"audio_id": "001", "transcribe_text": "test"}]

        with pytest.raises(req.exceptions.ConnectionError):
            refiner.refine(raw_input)

    @patch("src.refiner.requests.post")
    def test_refine_raises_on_http_error(self, mock_post, refiner):
        import requests as req
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = req.exceptions.HTTPError("500 Server Error")
        mock_post.return_value = mock_response

        raw_input = [{"audio_id": "001", "transcribe_text": "test"}]

        with pytest.raises(req.exceptions.HTTPError):
            refiner.refine(raw_input)

    @patch("src.refiner.requests.post")
    def test_refine_raises_on_invalid_json(self, mock_post, refiner):
        mock_response = MagicMock()
        mock_response.json.return_value = {"response": "not valid json {{{"}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        raw_input = [{"audio_id": "001", "transcribe_text": "test"}]

        with pytest.raises(json.JSONDecodeError):
            refiner.refine(raw_input)

    def test_refiner_custom_timeout(self):
        refiner = ASRRefiner(timeout=60)
        assert refiner.timeout == 60

    def test_refiner_default_timeout(self):
        refiner = ASRRefiner()
        assert refiner.timeout == 30
