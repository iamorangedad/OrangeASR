import pytest
from unittest.mock import MagicMock
import json

# --- Implementation ---


class ASRRefiner:
    def __init__(self, llm_client):
        self.llm_client = llm_client

    def refine_and_merge(self, raw_data):
        """
        Sends raw ASR segments to LLM and expects a JSON-formatted
        result containing the integrated audio_id and corrected text.
        """
        prompt = (
            "Analyze these ASR segments. The second segment often corrects the first. "
            "Return a single JSON object with the original 'audio_id' and the "
            "final corrected 'transcribe_text'. "
            f"Input: {json.dumps(raw_data)}"
        )

        # Simulate LLM returning a JSON string
        response_json = self.llm_client.ask_llm(prompt)
        return json.loads(response_json)


# --- Pytest Unit Test ---


class TestASRRefinement:

    @pytest.fixture
    def mock_llm(self):
        return MagicMock()

    def test_merge_correction_into_single_object(self, mock_llm):
        # 1. Setup raw input with correction context
        raw_input = [
            {"audio_id": "001", "transcribe_text": "direction to House Place"},
            {"audio_id": "002", "transcribe_text": "No, not house, it is Hearth"},
        ]

        # 2. Setup Mock LLM response as a JSON string
        # The LLM is expected to realize 001 is the primary ID and 002 is just a correction
        mock_response = json.dumps(
            {"audio_id": "001", "transcribe_text": "direction to Hearth Place"}
        )
        mock_llm.ask_llm.return_value = mock_response

        # 3. Execution
        refiner = ASRRefiner(mock_llm)
        result = refiner.refine_and_merge(raw_input)

        # 4. Assertions
        expected_result = {
            "audio_id": "001",
            "transcribe_text": "direction to Hearth Place",
        }

        assert result == expected_result
        assert result["audio_id"] == "001"
        assert "Hearth" in result["transcribe_text"]

        # Verify the LLM was actually prompted
        mock_llm.ask_llm.assert_called_once()

    def test_llm_parsing_error(self, mock_llm):
        # Test how the system handles malformed LLM output
        mock_llm.ask_llm.return_value = "Invalid JSON"
        refiner = ASRRefiner(mock_llm)

        with pytest.raises(json.JSONDecodeError):
            refiner.refine_and_merge([{"audio_id": "001", "text": "test"}])
