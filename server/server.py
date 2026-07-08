import asyncio
import websockets
import wave
import os
import time

async def audio_stream_handler(websocket):
    print(f"\n🔌 开发板已连接: {websocket.remote_address}")

    wav_file = None
    total_bytes = 0
    utterance_id = 0
    current_filename = None
    dropped_bytes = 0  # 统计因未收到 WAKE_UP 而被丢弃的数据量

    def open_new_wav():
        nonlocal utterance_id
        utterance_id += 1
        filename = f"utterance_{int(time.time())}_{utterance_id}.wav"
        f = wave.open(filename, "wb")
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(16000)
        return f, filename

    try:
        async for message in websocket:
            if isinstance(message, bytes):
                if wav_file is None:
                    # 严格模式：没有 WAKE_UP，音频一律丢弃，不写盘
                    dropped_bytes += len(message)
                    print(f"🚫 未收到 WAKE_UP，丢弃音频帧: {len(message)} 字节 | 累计丢弃: {dropped_bytes} 字节", end="\r")
                    continue

                wav_file.writeframes(message)
                total_bytes += len(message)
                print(f"📥 收到音频帧: {len(message)} 字节 | 累计: {total_bytes} 字节", end="\r")

            else:
                print(f"\n💬 收到文本信令: {message}")

                if message == "WAKE_UP":
                    if wav_file is not None:
                        # 上一段没正常收到 SPEECH_DONE 就来了新的 WAKE_UP，先关掉旧的
                        wav_file.close()
                        print(f"⚠️ 上一段未正常结束就收到新 WAKE_UP，强制关闭: {current_filename}")
                    wav_file, current_filename = open_new_wav()
                    total_bytes = 0
                    dropped_bytes = 0
                    print(f"🎙️ 收到 WAKE_UP，开始新录音: {current_filename}")

                elif message == "SPEECH_DONE":
                    if wav_file is not None:
                        wav_file.close()
                        print(f"💾 录音已保存: {os.path.abspath(current_filename)} ({total_bytes} 字节)")
                        # TODO: asyncio.create_task(process_utterance(current_filename))
                        wav_file = None
                    else:
                        print("⚠️ 收到 SPEECH_DONE 但当前没有正在录制的文件，忽略")

    except websockets.exceptions.ConnectionClosed:
        print(f"\nℹ️ 开发板连接断开")
    except Exception as e:
        print(f"\n❌ 捕获到其他异常: {e}")
    finally:
        if wav_file is not None:
            wav_file.close()
            if total_bytes == 0:
                try:
                    os.remove(current_filename)
                except Exception:
                    pass
            else:
                print(f"\n⚠️ 连接意外断开，但已保存未完成的录音: {current_filename}")


async def main():
    server = await websockets.serve(audio_stream_handler, "0.0.0.0", 8765)
    print("🚀 WebSocket 音频接收服务器已启动（严格模式：需先收到 WAKE_UP 才接收音频）")
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())