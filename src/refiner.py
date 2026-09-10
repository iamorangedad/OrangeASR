import json
import time
import asyncio
try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    httpx = None
    HAS_HTTPX = False
import requests


class ASRRefiner:
    def __init__(self, model_name="qwen3:4b", base_url="http://10.0.0.55:11434", timeout=30):
        self.model_name = model_name
        self.api_url = f"{base_url}/api/generate"
        self.timeout = timeout
        self._prompt_cache = ""
        self._failures = 0
        self._circuit_until = 0
        self._circuit_threshold = 3
        self._circuit_cooldown = 30

    def _build_prompt(self, segments):
        return f"""
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

    def _circuit_open(self):
        return time.time() < self._circuit_until

    def _record_success(self):
        self._failures = 0

    def _record_failure(self):
        self._failures += 1
        if self._failures >= self._circuit_threshold:
            self._circuit_until = time.time() + self._circuit_cooldown

    async def arefine(self, segments):
        if self._circuit_open():
            raise RuntimeError(f"Circuit breaker open until {self._circuit_until}")
        prompt = self._build_prompt(segments)
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0, "num_predict": 256, "top_k": 1},
        }
        last_err = None
        for attempt in range(3):
            try:
                if HAS_HTTPX:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        resp = await client.post(self.api_url, json=payload)
                        resp.raise_for_status()
                        full_response = resp.json()
                else:
                    loop = asyncio.get_running_loop()
                    def _sync():
                        r = requests.post(self.api_url, json=payload, timeout=self.timeout)
                        r.raise_for_status()
                        return r.json()
                    full_response = await loop.run_in_executor(None, _sync)
                model_output = full_response.get("response", "").strip()
                if not model_output:
                    model_output = full_response.get("thinking", "").strip()
                result = json.loads(model_output)
                self._record_success()
                return result
            except Exception as e:
                last_err = e
                if attempt < 2:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                self._record_failure()
                raise last_err

    def refine(self, segments):
        if self._circuit_open():
            raise RuntimeError(f"Circuit breaker open until {self._circuit_until}")
        prompt = self._build_prompt(segments)
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0, "num_predict": 256, "top_k": 1},
        }
        last_err = None
        for attempt in range(3):
            try:
                if HAS_HTTPX:
                    async def _do():
                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            resp = await client.post(self.api_url, json=payload)
                            resp.raise_for_status()
                            return resp.json()
                    try:
                        loop = asyncio.get_running_loop()
                        full_response = loop.run_until_complete(_do()) if False else asyncio.run(_do())
                    except RuntimeError:
                        full_response = asyncio.run(_do())
                else:
                    response = requests.post(self.api_url, json=payload, timeout=self.timeout)
                    response.raise_for_status()
                    full_response = response.json()
                model_output = full_response.get("response", "").strip()
                if not model_output:
                    model_output = full_response.get("thinking", "").strip()
                result = json.loads(model_output)
                self._record_success()
                return result
            except Exception as e:
                last_err = e
                if attempt < 2:
                    time.sleep(0.5 * (2 ** attempt))
                    continue
                self._record_failure()
                raise last_err
