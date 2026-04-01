import requests
import json


class ASRRefiner:
    """Calls Ollama LLM to refine and correct ASR transcription segments."""

    def __init__(self, model_name="qwen3:4b", base_url="http://10.0.0.55:11434", timeout=30):
        self.model_name = model_name
        self.api_url = f"{base_url}/api/generate"
        self.timeout = timeout

    def refine(self, segments):
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
            {json.dumps(segments, ensure_ascii=False)}

            ### Final JSON Output:
            """

        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0, "num_predict": 256, "top_k": 1},
        }

        response = requests.post(self.api_url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        full_response = response.json()

        model_output = full_response.get("response", "").strip()
        if not model_output:
            model_output = full_response.get("thinking", "").strip()

        return json.loads(model_output)
