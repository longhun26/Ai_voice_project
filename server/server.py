import asyncio
import websockets
import wave
import os

async def audio_stream_handler(websocket):
    print(f"\n🔌 开发板已连接: {websocket.remote_address}")
    
    # 准备写入 WAV 文件 (16kHz, 16bit, 单声道)
    wav_filename = "output.wav"
    wav_file = wave.open(wav_filename, "wb")
    wav_file.setnchannels(1)
    wav_file.setsampwidth(2) # 16-bit = 2 bytes
    wav_file.setframerate(16000)
    
    chunk_count = 0
    total_bytes = 0
    
    try:
        async for message in websocket:
            if isinstance(message, bytes):
                chunk_count += 1
                total_bytes += len(message)
                # 写入音频帧到文件
                wav_file.writeframes(message)
                print(f"📥 收到音频帧 #{chunk_count}: {len(message)} 字节 | 累计: {total_bytes} 字节", end="\r")
            else:
                print(f"\n💬 收到文本信令: {message}")
                
    except websockets.exceptions.ConnectionClosed as e:
        print(f"\nℹ️ 开发板传输正常结束 (ConnectionClosed)")
    except Exception as e:
        print(f"\n❌ 捕获到其他异常: {e}")
    finally:
        wav_file.close()
        if total_bytes > 0:
            print(f"💾 音频已成功保存至本地: {os.path.abspath(wav_filename)} ({total_bytes} 字节)")
        else:
            try: os.remove(wav_filename)
            except: pass

async def main():
    server = await websockets.serve(audio_stream_handler, "0.0.0.0", 8765)
    print("🚀 升级版 WebSocket 音频接收服务器已启动，等待 ESP32-S3 呼叫...")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())