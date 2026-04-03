import pytest
import requests
import json

class OllamaASRRefiner:
    def __init__(self, model_name="qwen3:4b", base_url="http://10.0.0.55:11434"):
        self.model_name = model_name
        self.api_url = f"{base_url}/api/generate"

    def refine_and_merge(self, raw_data):
        # Optimized prompt to capture both error and correction for fine-tuning purposes
        prompt = f"""
            ### Role
            You are an ASR Data Curator. Your goal is to identify errors in transcriptions and provide the cleaned version.

            ### Instructions
            1. **Analyze**: Look at all segments. Determine which part was a mistake and what the final intended sentence was.
            2. **Original Text**: Identify the initial incorrect transcription (usually the first main segment before corrections).
            3. **Corrected Text**: Synthesize the final, accurate sentence by applying all vocal corrections (like "No, it is X").
            4. **Output**: Return a JSON object with the primary ID, the raw error, and the final result.

            ### Output Format
            {{
                "audio_id": "string",
                "original_text": "the initial incorrect transcription",
                "corrected_text": "the final corrected version"
            }}

            ### Example
            Input: [
                {{"audio_id": "101", "transcribe_text": "Go to 5th Avenue"}}, 
                {{"audio_id": "102", "transcribe_text": "Wait, I said 6th Avenue"}}
            ]
            Output: {{
                "audio_id": "101", 
                "original_text": "Go to 5th Avenue", 
                "corrected_text": "Go to 6th Avenue"
            }}

            ### Current Task Input
            {json.dumps(raw_data, ensure_ascii=False)}

            ### Final JSON Output:
            """

        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0, "num_predict": 256, "top_k": 1},
        }
        
        model_output = ""
        try:
            response = requests.post(self.api_url, json=payload, timeout=30)
            response.raise_for_status()
            full_response = response.json()
            
            # Handling Qwen3 thinking/response field logic
            model_output = full_response.get("response", "").strip()
            if not model_output:
                model_output = full_response.get("thinking", "").strip()
            
            return json.loads(model_output)
        except requests.exceptions.RequestException as e:
            pytest.fail(f"Ollama connection failed: {e}")
        except json.JSONDecodeError:
            pytest.fail(f"Failed to parse LLM output as JSON. Raw output: {model_output}")


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

        # 2. Initialize refiner
        refiner = OllamaASRRefiner(model_name="qwen3:4b")

        # 3. Execution
        result = refiner.refine_and_merge(raw_input)
        print(f"\n[LLM Result]: {result}")

        # 4. Assertions for Fine-tuning Data Structure
        assert isinstance(result, dict)
        assert result.get("audio_id") == "001"
        
        # Verify both fields exist
        assert "original_text" in result
        assert "corrected_text" in result
        
        # Verify logic
        assert "House" in result["original_text"]
        assert "Hearth" in result["corrected_text"]
        assert "2900" in result["corrected_text"]
        assert "No, not house" not in result["corrected_text"]

    @pytest.mark.parametrize("model", ["qwen3:4b"])
    def test_different_models(self, model):
        refiner = OllamaASRRefiner(model_name=model)
        raw_input = [{"audio_id": "005", "transcribe_text": "Hello world"}]
        result = refiner.refine_and_merge(raw_input)
        assert result["audio_id"] == "005"
        assert result["corrected_text"] == "Hello world"