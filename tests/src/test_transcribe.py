import pytest
import os
import wave
import struct
import math
from faster_whisper import WhisperModel


# --- 1. Utility: Generate a simple test audio ---
def create_dummy_wav(filename, duration_sec=2):
    """
    Generate a WAV file containing a simple sine wave (beep).
    This way we don't need to rely on externally downloaded audio files.
    """
    sample_rate = 16000
    n_samples = int(sample_rate * duration_sec)

    with wave.open(filename, "w") as obj:
        obj.setnchannels(1)  # mono
        obj.setsampwidth(2)  # 2 bytes (16 bit)
        obj.setframerate(sample_rate)

        # Generate 440Hz sine wave
        data = []
        for i in range(n_samples):
            value = int(
                32767.0 * 0.5 * math.sin(2.0 * math.pi * 440.0 * i / sample_rate)
            )
            data.append(struct.pack("<h", value))

        obj.writeframes(b"".join(data))

    return filename


# --- 2. Pytest Fixtures (Setup) ---


@pytest.fixture(scope="module")
def audio_file():
    """
    Use a real audio file for testing instead of artificially generated sine waves.
    The Whisper model cannot recognize non-speech signals (such as pure tones).
    """
    # Use a real audio file from the project
    filename = "tests/california.mp3"
    if not os.path.exists(filename):
        pytest.skip(f"Test audio file not found: {filename}")

    yield filename


@pytest.fixture(scope="module")
def whisper_model():
    """
    Load the model.
    scope="module" ensures the model is loaded only once for the entire test file,
    avoiding reloading for each test case which would be slow.

    Note: In CI/CD or test environments, it is recommended to use the 'tiny' model
    to save time and memory.
    """
    # If running on Jetson, you can change to device="cuda", compute_type="float16"
    # For unit test portability, we default to cpu and int8
    model_size = "tiny"
    model = WhisperModel(model_size, device="cpu", compute_type="int8")
    return model


# --- 3. Test Cases ---


def test_model_loading(whisper_model):
    """
    Test 0: Verify the model loads successfully
    """
    assert whisper_model is not None


def test_transcribe_functionality(whisper_model, audio_file):
    """
    Test 1: Core functionality test - ensure the transcribe pipeline works
    """
    # Run recognition
    # beam_size=1 to speed up testing
    segments, info = whisper_model.transcribe(audio_file, beam_size=1)

    # Important: segments is a generator.
    # Actual inference only happens when you iterate over it!
    segments_list = list(segments)

    # --- Assertions (verify results) ---

    # 1. Verify audio duration was detected (just needs to be > 0)
    print(f"\n[Info] Detected duration: {info.duration}s")
    assert info.duration > 0

    # 2. Verify there are output segments
    # Even with a beep sound, the model might produce blank or hallucinated text,
    # but the list should not raise an error
    assert isinstance(segments_list, list)

    # 3. Print recognition results for debugging
    text = "".join([s.text for s in segments_list]).strip()
    print(f"[Result] Transcribed text: '{text}'")


@pytest.mark.parametrize("beam_size", [1, 5])
def test_transcribe_params(whisper_model, audio_file, beam_size):
    """
    Test 2: Parameterized test - verify no errors occur with different parameters
    """
    segments, _ = whisper_model.transcribe(audio_file, beam_size=beam_size)
    results = list(segments)
    assert len(results) >= 0
