import pyaudio
import wave
import time

def stream_audio_once(
    realtime_audio_file,
    transcribe_audio,
    postprocess_text,
    update_callback,
    buffer_duration=5,
    model_name="tiny"
):
    CHUNK = 1024
    FORMAT = pyaudio.paInt16
    CHANNELS = 1
    RATE = 16000

    p = pyaudio.PyAudio()
    stream = p.open(format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=CHUNK)
    frames = []

    try:
        for _ in range(0, int(RATE / CHUNK * buffer_duration)):
            data = stream.read(CHUNK, exception_on_overflow=False)
            frames.append(data)

        # 버퍼를 .wav로 저장
        wf = wave.open(realtime_audio_file, 'wb')
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(p.get_sample_size(FORMAT))
        wf.setframerate(RATE)
        wf.writeframes(b''.join(frames))
        wf.close()

        # Whisper로 변환
        text = transcribe_audio(realtime_audio_file, model_name=model_name)
        corrected_text = postprocess_text(text)
        update_callback(corrected_text)
    except Exception as e:
        update_callback(f"에러: {str(e)}")
    finally:
        stream.stop_stream()
        stream.close()
        p.terminate()