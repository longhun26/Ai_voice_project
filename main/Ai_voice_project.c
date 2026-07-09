/*
 * ESP32-S3-Korvo-1 v4.0 + ESP-ADF BSP 方案
 * Speech Recognition - VAD (Voice Activity Detection) 示例
 */

#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "esp_log.h"
#include "esp_check.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "nvs_flash.h"
#include "esp_websocket_client.h"
#include "freertos/ringbuf.h"

#include "bsp/esp-bsp.h"          // BSP: espressif/esp32_s3_korvo_1
#include "esp_codec_dev.h"

#include "esp_afe_sr_models.h"
#include "esp_afe_sr_iface.h"
#include "model_path.h"

static const char *TAG = "VAD_DEMO";
// 填入你的 Wi-Fi 和 电脑 IP 
#define ESP_WIFI_SSID      "CU_T76n"
#define ESP_WIFI_PASS      "zqfgc7q6"
#define PC_WEBSOCKET_URL   "ws://192.168.18.7:8765"

static esp_websocket_client_handle_t s_ws_client = NULL;
static RingbufHandle_t s_audio_ring_buf = NULL;
static bool s_is_forwarding = false; 

/* ----------------------- 配置参数 ----------------------- */
#define AUDIO_SAMPLE_RATE      16000
#define AUDIO_CHANNELS_MIC     4         
#define AUDIO_BITS             16
#define I2S_FRAME_MS           20         

/* ----------------------- 全局句柄 ----------------------- */
static esp_codec_dev_handle_t s_mic_dev = NULL;
static esp_afe_sr_iface_t    *s_afe_handle = NULL;
static esp_afe_sr_data_t     *s_afe_data = NULL;

static QueueHandle_t s_vad_state_queue = NULL;

typedef enum {
    VAD_EVENT_START,
    VAD_EVENT_END,
} vad_event_type_t;

typedef struct {
    vad_event_type_t type;
} vad_event_t;
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data) 
{
    static bool s_ws_initialized = false;
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW("WIFI", "Wi-Fi 断开，正在尝试重连...");
        esp_wifi_connect();
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {

        ip_event_got_ip_t* event = (ip_event_got_ip_t*) event_data;
        ESP_LOGI("WIFI", "连接成功！获取到 IP:" IPSTR, IP2STR(&event->ip_info.ip));
        if (!s_ws_initialized) {
        esp_websocket_client_config_t ws_cfg = {
            .uri = PC_WEBSOCKET_URL,
            .reconnect_timeout_ms = 3000,   // 断线后自动重连
            .network_timeout_ms = 5000,
        };
        s_ws_client = esp_websocket_client_init(&ws_cfg);
        esp_websocket_client_start(s_ws_client);
        s_ws_initialized = true;
        ESP_LOGI("WS", "WebSocket 客户端已启动");
        } else {
            ESP_LOGI("WS", "网络恢复，WebSocket 将自动重连");
        }   
    }
}

void network_init(void) {
    // 初始化 NVS（Wi-Fi 底层需要）
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    esp_event_handler_instance_t instance_any_id;
    esp_event_handler_instance_t instance_got_ip;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, &instance_any_id));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, &instance_got_ip));

    wifi_config_t wifi_config = {
        .sta = {
            .ssid = ESP_WIFI_SSID,
            .password = ESP_WIFI_PASS,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
    esp_wifi_set_ps(WIFI_PS_MIN_MODEM);       // 开启调制解调器省电模式，平滑电流波峰
    esp_wifi_set_max_tx_power(40);            // 将 Wi-Fi 最大发射功率减半（默认是 80，改为 40 约为 10dBm）
    ESP_LOGI("WIFI", "已限制 Wi-Fi 发射功率并开启 Modem PS 以减少射频干扰");
}
static void websocket_send_task(void *arg) {
    ESP_LOGI("WS_TASK", "异步网络发送任务已启动");
    while (1) {
        size_t item_size;
        // 阻塞等待 RingBuffer 里的音频数据
        char *item = (char *)xRingbufferReceive(s_audio_ring_buf, &item_size, portMAX_DELAY);
        if (item != NULL) {
            // 如果连接正常，直接发送二进制数据帧
            if (s_ws_client && esp_websocket_client_is_connected(s_ws_client)) {
                esp_websocket_client_send_bin(s_ws_client, item, item_size, portMAX_DELAY);
                vTaskDelay(pdMS_TO_TICKS(1)); 
            }
            // 释放 RingBuffer 内存
            vRingbufferReturnItem(s_audio_ring_buf, (void *)item);
        }
    }
}
/* ============================================================
 * 1. BSP 麦克风初始化
 * ============================================================ */
static esp_err_t mic_init(void)
{
    ESP_LOGI(TAG, "Init BSP I2C ...");
    ESP_ERROR_CHECK(bsp_i2c_init());
    vTaskDelay(pdMS_TO_TICKS(50)); 
    ESP_LOGI(TAG, "Init BSP microphone (ES7210) ...");
    s_mic_dev = bsp_audio_codec_microphone_init();
    if (s_mic_dev == NULL) {
        ESP_LOGE(TAG, "Failed to init microphone codec device");
        return ESP_FAIL;
    }

    esp_codec_dev_sample_info_t fs = {
        .sample_rate     = AUDIO_SAMPLE_RATE,
        .channel         = AUDIO_CHANNELS_MIC, /* 传入 4 通道 */
        .bits_per_sample = AUDIO_BITS,
        .channel_mask    = (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3),
    };
    ESP_RETURN_ON_ERROR(esp_codec_dev_open(s_mic_dev, &fs), TAG, "codec open fail");

    esp_codec_dev_set_in_gain(s_mic_dev, 30.0);

    ESP_LOGI(TAG, "Mic init OK: %dHz %dch %dbit", fs.sample_rate, fs.channel, fs.bits_per_sample);
    return ESP_OK;
}

/* ============================================================
 * 2. AFE + VAD 初始化
 * ============================================================ */
static esp_err_t afe_vad_init(void)
{   
    srmodel_list_t *models = esp_srmodel_init("model");
    if (models == NULL) {
        ESP_LOGE("AFE", "Failed to init models");
        return ESP_FAIL;
    }
    afe_config_t *afe_config = afe_config_init("MMRN", models, AFE_TYPE_SR, AEC_MODE_FD_HIGH_PERF);
    if (afe_config == NULL) {
        ESP_LOGE("AFE", "Failed to init afe config");
        return ESP_FAIL;
    }
    afe_config->wakenet_init = false;   // 明确关闭，纯 VAD 模式不需要
    // 🌟 【核心微调：给底层 AFE 降噪】
    afe_config->vad_init = true;
    afe_config->ns_init = true;
    // 1. 乐鑫官方定义：vad_mode 值越小，语音触发概率越低（越不敏感）。默认通常是 VAD_MODE_1 或 2，调小它
    afe_config->vad_mode = VAD_MODE_2; 
    // 2. 提高判定为人声的最小持续时间（默认128ms）。
    // 将其拉长到 240ms 甚至 300ms，这样耗时极短的突发杂音（如敲击、爆音）直接在底层就被过滤掉了
    afe_config->vad_min_speech_ms = 240; 
    // 3. 判定为持续噪音/静音的最小时间（默认1000ms），可保持或微调
    afe_config->vad_min_noise_ms = 800;
    afe_config->pcm_config.total_ch_num = AUDIO_CHANNELS_MIC; 
    afe_config->pcm_config.mic_num = 2;
    afe_config->pcm_config.ref_num = 1;
    afe_config->agc_init = true;
    afe_config->agc_mode = 2;// 开启 AFE 级别的 AGC
    afe_config_print(afe_config); 
    s_afe_handle = esp_afe_handle_from_config(afe_config);
    if (s_afe_handle == NULL) {
        ESP_LOGE("AFE", "Failed to get afe handle");
        afe_config_free(afe_config);
        return ESP_FAIL;
    }
    s_afe_data = s_afe_handle->create_from_config(afe_config);
    if (s_afe_data == NULL) {
        ESP_LOGE(TAG, "AFE create failed");
        afe_config_free(afe_config);
        return ESP_FAIL;
    }
    
    afe_config_free(afe_config);
    ESP_LOGI("AFE", "AFE VAD initialized successfully");
    return ESP_OK;
}

/* ============================================================
 * 3. 音频采集 Task
 * ============================================================ */
static void audio_feed_task(void *arg)
{
    int feed_chunksize = s_afe_handle->get_feed_chunksize(s_afe_data);
    int feed_channels = s_afe_handle->get_feed_channel_num(s_afe_data);
    
    int16_t *feed_buf = heap_caps_malloc(
        feed_chunksize * feed_channels * sizeof(int16_t),
        MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);

    if (feed_buf == NULL) {
        ESP_LOGE(TAG, "feed_buf alloc failed");
        vTaskDelete(NULL);
        return;
    }

    ESP_LOGI(TAG, "audio_feed_task started");

    while (1) {
        esp_err_t ret = esp_codec_dev_read(
            s_mic_dev, feed_buf,
            feed_chunksize * feed_channels * sizeof(int16_t));

        if (ret != ESP_OK) {
            ESP_LOGW(TAG, "codec read err: %d", ret);
            vTaskDelay(pdMS_TO_TICKS(5));
            continue;
        }

        s_afe_handle->feed(s_afe_data, feed_buf);
        vTaskDelay(1); 
    }

    free(feed_buf);
    vTaskDelete(NULL);
}

/* ============================================================
 * 4. VAD 检测 Task
 * ============================================================ */
static void afe_fetch_task(void *arg)
{
    ESP_LOGI(TAG, "afe_fetch_task started (纯 VAD 触发模式)");
    bool last_state = false;

    while (1) {
        afe_fetch_result_t *res = s_afe_handle->fetch(s_afe_data);

        if (!res || res->ret_value == ESP_FAIL) {
            vTaskDelay(pdMS_TO_TICKS(20));
            continue;
        }

        /* 1. VAD 状态机检测（取代原有的唤醒词逻辑） */
        bool speech_now = (res->vad_state == VAD_SPEECH);
        
        if (speech_now != last_state) {
            ESP_LOGI(TAG, "VAD状态变更: %s", speech_now ? "有人说话 START" : "没人说话 END");
            
            if (speech_now) {
                s_is_forwarding = true;
               vad_event_t evt = {
                .type = VAD_EVENT_START,
                };
                xQueueSend(s_vad_state_queue, &evt, 0);
            } else {
                // 🛑 【没人说话】：关闭大门，停止转发
                if (s_is_forwarding) {
                    s_is_forwarding = false;
                    vad_event_t evt = {
                    .type = VAD_EVENT_END,
                    };
                    xQueueSend(s_vad_state_queue, &evt, 0);
                }
            }
            last_state = speech_now;
        }
        if (s_is_forwarding && res->data != NULL && res->data_size > 0) {
            // 将 AFE 处理完的 16kHz/16bit 单声道 PCM 塞入环形缓冲区
            BaseType_t rem = xRingbufferSend(s_audio_ring_buf, res->data, res->data_size, 0);
            if (rem != pdTRUE) {
                ESP_LOGE(TAG, "缓冲区满！丢弃当前音频帧");
            }
        }
    }
    vTaskDelete(NULL);
}

/* ============================================================
 * 5. 下游消费 Task
 * ============================================================ */
static void vad_consumer_task(void *arg)
{
    vad_event_t evt;

    while (1) {
        if (xQueueReceive(s_vad_state_queue, &evt, portMAX_DELAY) == pdTRUE) {

            if (!s_ws_client ||
                !esp_websocket_client_is_connected(s_ws_client)) {
                continue;
            }
            switch (evt.type) {

            case VAD_EVENT_START:

                ESP_LOGI(TAG, "Send WAKE_UP");
                esp_websocket_client_send_text(
                    s_ws_client,
                    "WAKE_UP",
                    strlen("WAKE_UP"),
                    pdMS_TO_TICKS(100));
                break;

            case VAD_EVENT_END:

                ESP_LOGI(TAG, "Send SPEECH_DONE");
                esp_websocket_client_send_text(
                    s_ws_client,
                    "SPEECH_DONE",
                    strlen("SPEECH_DONE"),
                    pdMS_TO_TICKS(100));
                break;
            }
        }
    }
}


/* ============================================================
 * app_main
 * ============================================================ */
void app_main(void)
{
    ESP_LOGI(TAG, "=== ESP32-S3-Korvo-1 v4.0 VAD Demo (BSP + ESP-ADF/ESP-SR) ===");
    ESP_LOGI(TAG, "Waiting for hardware power stabilization...");
    vTaskDelay(pdMS_TO_TICKS(500));

    // 2. 初始化网络（连接 Wi-Fi 并准备 WebSocket）
    network_init();

    // 3. 创建 64KB 的流式音频 RingBuffer（用于平滑网络抖动）
    s_audio_ring_buf = xRingbufferCreateWithCaps(64 * 1024, RINGBUF_TYPE_NOSPLIT, MALLOC_CAP_SPIRAM);
    if (s_audio_ring_buf == NULL) {
        ESP_LOGE("MAIN", "环形缓冲区创建失败！");
        return;
    }
    ESP_ERROR_CHECK(mic_init());
    ESP_ERROR_CHECK(afe_vad_init());

    s_vad_state_queue = xQueueCreate(10, sizeof(vad_event_t));
    configASSERT(s_vad_state_queue);
   
    xTaskCreatePinnedToCore(websocket_send_task, "ws_send_task", 4 * 1024, NULL, 5, NULL, 0);
    xTaskCreatePinnedToCore(audio_feed_task,   "audio_feed",   4096, NULL, 5, NULL, 0);
    xTaskCreatePinnedToCore(afe_fetch_task,    "afe_fetch",    4096, NULL, 5, NULL, 1);
    xTaskCreatePinnedToCore(vad_consumer_task, "vad_consumer", 4096, NULL, 4, NULL, 0);

    ESP_LOGI(TAG, "Say something to trigger VAD_SPEECH ...");
}