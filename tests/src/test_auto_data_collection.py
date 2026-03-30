import pytest
from unittest.mock import MagicMock
import json

# --- Implementation ---


class ASRProcessor:
    def __init__(self, llm_client):
        self.llm_client = llm_client

    def refine_transcripts(self, raw_data):
        """
        Integrates raw ASR segments into a final corrected text using LLM.
        """
        prompt = (
            "You are an ASR post-processing assistant. "
            "Please integrate the following segments, handling any self-corrections: "
            f"{json.dumps(raw_data)}"
        )

        # Call the mocked LLM client
        response = self.llm_client.generate_text(prompt)
        return response


# --- Pytest Unit Test ---


class TestASRIntegration:

    @pytest.fixture
    def mock_llm(self):
        """Fixture to create a mock LLM client."""
        client = MagicMock()
        return client

    def test_correction_logic(self, mock_llm):
        # 1. Define raw ASR input with errors and corrections
        input_data = [
            {"audio_id": "001", "transcribe_text": "direction to House Place"},
            {"audio_id": "002", "transcribe_text": "No, not house, it is Hearth"},
        ]

        # 2. Set the expected behavior for the mock
        expected_output = "direction to Hearth Place"
        mock_llm.generate_text.return_value = expected_output

        # 3. Initialize the processor
        processor = ASRProcessor(mock_llm)

        # 4. Execute the logic
        final_result = processor.refine_transcripts(input_data)

        # 5. Assertions
        assert final_result == expected_output
        mock_llm.generate_text.assert_called_once()

    def test_empty_input(self, mock_llm):
        # Additional test case for empty input handling
        mock_llm.generate_text.return_value = ""
        processor = ASRProcessor(mock_llm)

        result = processor.refine_transcripts([])

        assert result == ""
