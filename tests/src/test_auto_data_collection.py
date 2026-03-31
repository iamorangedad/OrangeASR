import pytest
import requests
import json


class OllamaASRRefiner:
    def __init__(self, model_name="qwen3:4b", base_url="http://10.0.0.55:11434"):
        self.model_name = model_name
        self.api_url = f"{base_url}/api/generate"

    def refine_and_merge(self, raw_data):
        """
        Connects to a local Ollama instance to process ASR corrections.
        """

        prompt = f"""
            ### Role
            You are a high-precision ASR Transcription Auditor. Your task is to synthesize multiple audio segments into a single, accurate ground-truth record for AI training.

            ### Logic & Reasoning Steps
            1. **Conflict Detection**: Analyze all segments. Identify which segment contains a "misrecognition" and which segment provides a "correction" (look for cues like "No", "It is", "I mean", or phonetic spelling like "H E A R T H").
            2. **Context Preservation**: Retain all non-conflicting information (e.g., numbers, addresses, or intent) from all segments.
            3. **ID Selection**: Always use the 'audio_id' from the very first segment in the sequence as the primary identifier.
            4. **Final Synthesis**: Construct a fluent, corrected sentence that represents the user's final intended meaning.

            ### Output Constraint
            - Return ONLY a valid JSON object.
            - NO "thinking" text, NO explanations.
            - JSON structure: {{"audio_id": "string", "transcribe_text": "string"}}

            ### Examples
            Input: [
                {{"audio_id": "101", "transcribe_text": "Go to 5th Avenue"}}, 
                {{"audio_id": "102", "transcribe_text": "Wait, I said 6th Avenue, not 5th"}}
            ]
            Output: {{"audio_id": "101", "transcribe_text": "Go to 6th Avenue"}}

            Input: [
                {{"audio_id": "201", "transcribe_text": "My name is John"}}, 
                {{"audio_id": "202", "transcribe_text": "J O H N N Y"}},
                {{"audio_id": "203", "transcribe_text": "Sorry, it is Johnny"}}
            ]
            Output: {{"audio_id": "201", "transcribe_text": "My name is Johnny"}}

            ### Current Task Input
            {json.dumps(raw_data, ensure_ascii=False)}

            ### Final JSON Output:
            """

        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0, "num_predict": 128, "top_k": 1},
        }
        try:
            response = requests.post(self.api_url, json=payload, timeout=30)
            response.raise_for_status()
            full_response = response.json()
            model_output = full_response.get("response", "").strip()
            if not model_output:
                model_output = full_response.get("thinking", "").strip()
            return json.loads(model_output)
        except requests.exceptions.RequestException as e:
            pytest.fail(f"Ollama connection failed: {e}")
        except json.JSONDecodeError:
            pytest.fail(f"Failed to parse LLM output as JSON: {model_output}")


class TestOllamaASRIntegration:

    def test_live_ollama_correction(self):
        # 1. Setup the input data
        raw_input = [
            {"audio_id": "001", "transcribe_text": "direction to House Place 2900"},
            {
                "audio_id": "002",
                "transcribe_text": "No, not house, it is Hearth, H E A R T H.",
            },
        ]

        # 2. Initialize refiner pointing to local Ollama
        # Ensure the model specified here is already pulled via 'ollama pull'
        refiner = OllamaASRRefiner(model_name="qwen3:4b")

        # 3. Execution
        result = refiner.refine_and_merge(raw_input)
        print(result)

        # 4. Assertions based on LLM reasoning
        # We expect the LLM to identify '001' as the primary ID
        assert isinstance(result, dict)
        assert result.get("audio_id") == "001"

        # Check if 'Hearth' replaced 'House'
        final_text = result.get("transcribe_text", "")
        assert "Hearth" in final_text
        assert "No, not house" not in final_text

    @pytest.mark.parametrize("model", ["qwen3:4b", "qwen3:0.6b"])
    def test_different_models(self, model):
        # Optional: Test against multiple local models if available
        refiner = OllamaASRRefiner(model_name=model)
        raw_input = [{"audio_id": "005", "transcribe_text": "Hello world"}]
        result = refiner.refine_and_merge(raw_input)
        assert result["audio_id"] == "005"
