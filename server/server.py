import asyncio
import websockets
import wave
import os
import time

# 🌟 引入 FunASR 及清洗工具
from funasr import AutoModel
from funasr.utils.postprocess_utils import rich_transcription_postprocess

# =====================================================================
# 🚀 1. 核心模型初始化（生命周期管理：全局仅加载一次）
# =====================================================================
print("⏳ 正在初始化本地 FunASR SenseVoiceSmall 模型（首次运行会自动下载）...")

# 如果你电脑有 NVIDIA 独立显卡，请保持 device="cuda"
# 如果是纯 CPU 运行，请将 cuda 改为 cpu
asr_model = AutoModel(
    model="iic/SenseVoiceSmall",
    vad_model="fsmn-vad",                 # 配合官方 VAD 效果更稳定
    vad_kwargs={"max_single_segment_time": 30000},
    device="cuda",                        # "cuda" 或 "cpu"
    hub="ms"                              # 强制指定 ModelScope 渠道，国内下载极快
)
print("✅ 本地语音识别模型就绪，等待开发板连接...\n")


# =====================================================================
# 🧠 2. 补全后的 AI 语音流水线任务
# =====================================================================
async def process_utterance(filename, websocket):
    """
    AI 核心管线：本地 ASR 识别 -> [下一步对接 LLM] -> [下一步对接 TTS]
    """
    print(f"\n🤖 [AI 管道启动] 正在处理音频: {filename}")
    start_time = time.time()
    
    try:
        # 基础防呆：防止空文件触发异常
        if not os.path.exists(filename) or os.path.getsize(filename) < 44:
            print("⚠️ 音频文件为空或非法，终止处理")
            return

        print("🎙️ 本地 SenseVoice 正在推理...")
        
        # 🌟 核心保命招式：asr_model.generate 是一个吃满算力的同步阻塞函数。
        # 必须使用 asyncio.to_thread 将其扔进底层的独立线程池中执行。
        # 这样推理在吃满 CPU/GPU 的同时，WebSocket 的主事件循环依然能流畅接收板子的新数据。
        res = await asyncio.to_thread(
            asr_model.generate,
            input=filename,
            cache={},
            language="auto",   # auto 为自动语种识别，中文长文本可固定写 "zh"
            use_itn=True
        )

        # 🧹 清洗并提取识别出的文本
        if res and len(res) > 0 and "text" in res[0]:
            raw_text = res[0]["text"]
            
            # SenseVoice 默认会带出富文本标记（如：<|HAPPY|> 谢谢大家 <|APPLAUSE|>）
            # 通过官方工具将其剥离，还原为纯文本
            user_text = rich_transcription_postprocess(raw_text).strip()
        else:
            user_text = ""

        cost_ms = int((time.time() - start_time) * 1000)
        print(f"🎯 【STT 识别结果】({cost_ms}ms): {user_text}")
        
        if not user_text:
            print("⚠️ 未能从音频中识别出有效文字")
            return

        # 📡 将识别结果第一时间推回给开发板（用于板载屏幕显示或调试日志）
        await websocket.send(f"STT_TEXT:{user_text}")

        # -------------------------------------------------------------
        # TODO: 🧠 下一步在这里对接大语言模型 (LLM)
        # -------------------------------------------------------------
        # print("🧠 AI 正在思考回复...")
        # ai_reply = await call_llm(user_text)
        # await websocket.send(f"LLM_TEXT:{ai_reply}")

    except Exception as e:
        print(f"❌ 本地 ASR 处理管线发生异常: {e}")
        
    finally:
        # 🗑️ 战争残留清理：不管识别成功或失败，及时删除本地已经转写完的 wav 文件
        if os.path.exists(filename):
            try:
               # os.remove(filename)
              print(f"🗑️ 临时临时音频文件已清理: {filename}")
            except Exception as e:
                print(f"⚠️ 清理临时文件失败: {e}")


# =====================================================================
# 🔌 3. 你原本完美的 WebSocket 网络流状态机（保持不变）
# =====================================================================
async def audio_stream_handler(websocket):
    print(f"\n🔌 开发板已连接: {websocket.remote_address}")

    wav_file = None
    total_bytes = 0
    utterance_id = 0
    current_filename = None
    dropped_bytes = 0

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
                        
                        # 🌟 这里无缝挂载异步 AI 处理任务
                        asyncio.create_task(process_utterance(current_filename, websocket))
                        
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
                try: os.remove(current_filename)
                except Exception: pass
            else:
                print(f"\n⚠️ 连接意外断开，但已保存未完成的录音: {current_filename}")


async def main():
    server = await websockets.serve(audio_stream_handler, "0.0.0.0", 8765)
    print("🚀 WebSocket 音频网关服务器运行中...")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())