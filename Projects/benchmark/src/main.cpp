/*
 * ============================================================
 * ESP32 DISTRIBUTED MPI KERNEL  (WiFi/UDP  ou  ESP-NOW)
 *   + BENCHMARK: MÁXIMO DE UM VETOR
 *   + MEDIÇÃO DE PERDA DE PACOTES POR EXECUÇÃO
 * ============================================================
 *
 * Camada de comunicação (todos os nodes com o MESMO modo):
 *     -DCOMM_MODE=0   Wi-Fi/UDP (roteador)
 *     -DCOMM_MODE=1   ESP-NOW   (sem roteador)
 *
 * Todos os nodes devem usar o MESMO TOTAL_APPLICATION_TASKS (12)
 * e o MESMO NUM_NODES (1..5). Só THIS_NODE_ID muda entre as placas.
 *
 * Distribuição dos ranks:  rank % NUM_NODES == THIS_NODE_ID
 * Node 0 é o coordenador da barreira e o master.
 *
 * ------------------------------------------------------------
 * BENCHMARK
 * ------------------------------------------------------------
 * Vetor fixo de 100.000 elementos (int16). Para cada número de
 * ranks ativos R em {1,2,4,8,16,32}, repete BENCH_REPS vezes:
 *
 *   1. Rank 0 gera o vetor (em streaming, bloco a bloco) e envia
 *      a fatia de cada rank 1..R-1 (blocos de 128 bytes = 64 int16).
 *   2. Cada rank calcula o máximo da sua fatia (rank 0 calcula a própria).
 *   3. Ranks 1..R-1 enviam só o máximo parcial (+ seus tempos).
 *   4. Rank 0 faz a redução M = max(M0, M1, ... , M(R-1)).
 *   5. Rank 0 confere M contra o máximo de referência acumulado
 *      durante a geração (detecta mensagem perdida/duplicada).
 *
 * Os 32 ranks existem sempre; os ranks >= R ficam "inativos"
 * (só participam das barreiras e do MPI_Bcast final).
 *
 * Tempos (rank 0, em ms): Ttotal, Tgen, Tcomm, Tcalc0/TcalcMax,
 * Tred, TcommMax. Speedup = T1/TR ; Eficiência = Speedup/R.
 *
 * Perda de pacotes: reportada como NÚMERO DE RETRANSMISSÕES
 * (tentativas de envio - mensagens), por link e por execução.
 *
 * Saída máquina-legível:  linhas [CSV_RUN] e [CSV_RES].
 * ============================================================
 */

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiUdp.h>
#include <esp_now.h>
#include <esp_wifi.h>

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>

#include <esp_timer.h>
#include <esp_heap_caps.h>


// ============================================================
// CONFIGURAÇÃO DO KERNEL
// ============================================================

#define COMM_WIFI                   0
#define COMM_ESPNOW                 1

#ifndef COMM_MODE
#define COMM_MODE                   COMM_ESPNOW
#endif

#define ESPNOW_CHANNEL              1
#define ESPNOW_RX_QUEUE_LENGTH      16

#ifndef THIS_NODE_ID
#define THIS_NODE_ID                0       // <<< 1, 2, 3, 4 nas outras placas
#endif

#ifndef NUM_NODES
#define NUM_NODES                   1       // 1 = standalone (sem rádio)
#endif

// 32 ranks existem sempre; o benchmark ativa 1,2,4,8,16,32 deles.
#ifndef TOTAL_APPLICATION_TASKS
#define TOTAL_APPLICATION_TASKS     16
#endif

#define SINGLE_NODE_MODE            (NUM_NODES == 1)

static_assert(THIS_NODE_ID >= 0 && THIS_NODE_ID < NUM_NODES, "THIS_NODE_ID invalido");
static_assert(COMM_MODE == COMM_WIFI || COMM_MODE == COMM_ESPNOW, "COMM_MODE invalido");
static_assert(TOTAL_APPLICATION_TASKS >= NUM_NODES,
              "precisa de ao menos 1 rank por node (rank == THIS_NODE_ID faz as estatisticas)");

#define DATA_PORT                   5000
#define DISCOVERY_PORT              5001
#define CONTROL_PORT                5002

#define MESSAGE_PAYLOAD             128

#define QUEUE_LENGTH                8
#define SENDER_QUEUE_LENGTH         32
#define PENDING_MAX                 8

// Com 32 ranks a RAM aperta: ranks "de estatistica/impressao" (rank < NUM_NODES,
// inclui o rank 0) usam stack grande; os demais usam stack pequena.
// 32 ranks em UM unico node ESP32 sem PSRAM provavelmente nao cabem na RAM:
// use 2+ nodes (ou placa com PSRAM).
#define APP_STACK_SIZE              8192
#define APP_STACK_SIZE_SMALL        4096
#define KERNEL_STACK_SIZE           6144

#define TASK_PRIORITY               1
#define KERNEL_TASK_PRIORITY        3
#define MASTER_TASK_PRIORITY        3

// Confiabilidade
#define DATA_ACK_TIMEOUT_MS         100
#define DATA_MAX_RETRIES            10
#define BARRIER_RESEND_MS           300
#define CONTROL_ACK_TIMEOUT_MS      2000
#define CONTROL_MAX_RETRIES         5

#define DEBUG_STACK                 0
#define MPI_ANY_SOURCE              (-1)

// 1 = imprime chegada/liberação de barreira (muito verboso, 12 ranks)
#ifndef KERNEL_VERBOSE
#define KERNEL_VERBOSE              0
#endif

#ifndef STATS_PRINT_PERIOD_MS
#define STATS_PRINT_PERIOD_MS       0       // >0 = imprime stats locais no loop()
#endif

#define STATS_EXCHANGE_MS           4000    // troca final (acumulado)
#define STATS_EXCHANGE_PERIOD_MS    500


// ============================================================
// CONFIGURAÇÃO WI-FI  (usada só em COMM_WIFI)
// ============================================================

const char* WIFI_SSID     = "Wifi Leonardo";
const char* WIFI_PASSWORD = "qwertyuiop";


// ============================================================
// TIPOS
// ============================================================

enum MessageType
{
    MSG_APPLICATION = 1,
    MSG_DATA_ACK    = 2
};

typedef struct __attribute__((packed))
{
    uint8_t  type;
    int32_t  srcRank;
    int32_t  dstRank;
    uint32_t sequence;
    uint16_t length;
    char     payload[MESSAGE_PAYLOAD];
} Message;

enum ControlType
{
    CONTROL_CREATE_TASK     = 1,
    CONTROL_CREATE_ACK      = 2,
    CONTROL_START           = 3,
    CONTROL_START_ACK       = 4,
    CONTROL_BARRIER         = 5,
    CONTROL_BARRIER_RELEASE = 6,
    CONTROL_STATS           = 7     // snapshot de estatísticas do link
};

typedef struct __attribute__((packed))
{
    uint8_t  type;
    uint8_t  srcNode;
    int32_t  rank;
    int32_t  generation;    // em CONTROL_STATS: 0 = acumulado, >0 = id da execução
    int32_t  status;
    uint32_t stat[4];       // só usado em CONTROL_STATS
} ControlMessage;

#define DISCOVERY_MAGIC 0xA5

typedef struct __attribute__((packed))
{
    uint8_t magic;
    uint8_t nodeId;
    uint8_t ip[4];
} DiscoveryMessage;

typedef struct
{
    int rank;
    TaskHandle_t      taskHandle;
    QueueHandle_t     receiveQueue;

    SemaphoreHandle_t barrierSem;
    int32_t           barrierGen;

    Message          *pending;
    int               pendingCount;

    volatile bool     finalized;
} TaskContext;

typedef struct
{
    IPAddress ip;
    uint8_t   mac[6];
} PeerAddr;

enum CommChannel
{
    CH_DATA      = 0,
    CH_CONTROL   = 1,
    CH_DISCOVERY = 2
};

// ---- Estatísticas por link (indexado pelo node do OUTRO lado) ----
typedef struct
{
    // escritos só pelo TaskSender
    volatile uint32_t msgs;         // mensagens remotas processadas
    volatile uint32_t txAttempts;   // pacotes de dados que realmente saíram
    volatile uint32_t txSendFail;   // falha LOCAL de TX (nao e perda no ar)
    volatile uint32_t ackOk;        // tentativas que receberam ACK
    volatile uint32_t msgFailed;    // mensagens perdidas de vez (sem ACK)

    // escritos só pelo TaskReceiver
    volatile uint32_t rxData;       // pacotes de dados validos recebidos
    volatile uint32_t rxUnique;     // entregues pela 1a vez
    volatile uint32_t rxDup;        // duplicatas (ACK anterior se perdeu)
    volatile uint32_t rxQueueFull;  // descartados: fila do rank cheia
    volatile uint32_t ackTx;        // ACKs enviados
    volatile uint32_t ackRx;        // ACKs recebidos
} LinkStats;

// Snapshot enviado pelo vizinho (visão DELE do link com este node)
typedef struct
{
    uint32_t txAttempts;    // dados que ele enviou a mim
    uint32_t rxData;        // dados que ele recebeu de mim
    uint32_t ackTx;         // ACKs que ele enviou a mim
    uint32_t ackRx;         // ACKs que ele recebeu de mim
    volatile bool valid;
} PeerSnapshot;


// ============================================================
// PROTÓTIPOS
// ============================================================

void ApplicationTask(void *parameter);

void TaskSender(void *parameter);
void TaskReceiver(void *parameter);
void TaskDiscovery(void *parameter);
void TaskControl(void *parameter);
void TaskMaster(void *parameter);

void Stats_Reset();
void Stats_PrintLocal();
void Stats_ExchangeAndPrint();
void Bench_CalibrateTimer();


// ============================================================
// ESTADO GLOBAL
// ============================================================

TaskContext taskContexts[TOTAL_APPLICATION_TASKS];

WiFiUDP udpDataRx;
WiFiUDP udpDataTx;
WiFiUDP udpDiscoveryRx;
WiFiUDP udpDiscoveryTx;
WiFiUDP udpControlRx;
WiFiUDP udpControlTx;

SemaphoreHandle_t dataTxMutex;
SemaphoreHandle_t controlTxMutex;
SemaphoreHandle_t discoveryTxMutex;

IPAddress nodeIP[NUM_NODES];
uint8_t   nodeMAC[NUM_NODES][6];
volatile bool nodeDiscovered[NUM_NODES];

QueueHandle_t senderQueue;

SemaphoreHandle_t dataAckSemaphore;
volatile uint32_t lastAckSeq = 0;

int64_t lastDeliveredSeq[NUM_NODES];

SemaphoreHandle_t createAckSemaphore;
SemaphoreHandle_t startAckSemaphore;
volatile int lastCreateAckRank   = -1;
volatile int lastCreateAckStatus = 0;
volatile int lastStartAckStatus  = 0;

volatile bool kernelReady = false;
// ---------------- Tipos -------------------------------------

typedef struct
{
    int64_t  maxVal;
    uint32_t commUs;      // tempo do worker em MPI_Recv (inclui espera pelos dados)
    uint32_t calcUs;      // tempo do worker calculando o máximo da fatia
    int32_t  rank;        // quem enviou (a redução recebe de qualquer origem)
} BenchPartial;

typedef struct
{
    double  totalUs, genUs, commUs, calcUs, redUs;   // rank 0
    int64_t global, ref;
    int     sendFail;
    bool    ok;
    uint32_t rankCalcUs[TOTAL_APPLICATION_TASKS];
    uint32_t rankCommUs[TOTAL_APPLICATION_TASKS];
} BenchRaw;

typedef struct
{
    uint64_t msgs;        // mensagens remotas enviadas
    uint64_t tx;          // tentativas de envio que saíram
    uint64_t retx;        // retransmissões = tentativas - mensagens
    uint64_t failed;      // mensagens perdidas de vez (sem ACK)
} RetxAccum;

typedef struct
{
    int    R, reps;
    double total, gen, comm, calc0, calcMax, commMax, red;   // médias em ms
    double speedup, eff, speedupCalc;
    uint64_t msgs, tx, retx, failed;                         // totais (node 0, todas as execuções)
    bool   ok;
} ResultRow;

static ResultRow gRows[6];
static int       gRowCount = 0;
static double    gT1Total = 0;
static double    gT1Calc  = 0;

static double gTimerOvhUs = 0.0;     // custo de uma janela de medição vazia

// ---- Barreira ------------------------------------------------
int32_t coordGeneration = 0;
int32_t barrierArrivedGeneration[TOTAL_APPLICATION_TASKS];
volatile int32_t barrierWaitGen[TOTAL_APPLICATION_TASKS];
SemaphoreHandle_t barrierMutex;

// ---- Sequência ------------------------------------------------
volatile uint32_t nextSequence = 0;
static portMUX_TYPE sequenceMux = portMUX_INITIALIZER_UNLOCKED;

// ---- Estatísticas ---------------------------------------------
LinkStats    linkStats[NUM_NODES];
PeerSnapshot peerSnap[NUM_NODES];       // snapshot acumulado (generation == 0)
PeerSnapshot peerRun[NUM_NODES];        // snapshot de uma execução (generation > 0)
volatile int32_t peerRunGen[NUM_NODES];

volatile uint32_t espnowRxDrop = 0;   // fila RX do ESP-NOW cheia
volatile uint32_t macTxOk      = 0;   // resultado MAC (unicast)
volatile uint32_t macTxFail    = 0;


// ============================================================
// AUXILIARES
// ============================================================

static inline TickType_t Ms(uint32_t ms)
{
    TickType_t t = pdMS_TO_TICKS(ms);
    return (t == 0) ? 1 : t;
}

bool IsLocalRank(int rank)
{
    if (rank < 0 || rank >= TOTAL_APPLICATION_TASKS) return false;
    return (rank % NUM_NODES) == THIS_NODE_ID;
}

int RankToNode(int rank)
{
    return rank % NUM_NODES;
}

TaskContext *GetTaskContext(int rank)
{
    if (rank < 0 || rank >= TOTAL_APPLICATION_TASKS) return nullptr;
    return &taskContexts[rank];
}

TaskContext *GetCurrentTaskContext()
{
    TaskHandle_t h = xTaskGetCurrentTaskHandle();

    for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
    {
        if (taskContexts[i].taskHandle == h)
            return &taskContexts[i];
    }
    return nullptr;
}

uint32_t NextMessageSequence()
{
    uint32_t value;

    portENTER_CRITICAL(&sequenceMux);
    value = nextSequence++;
    portEXIT_CRITICAL(&sequenceMux);

    return value;
}

bool CreateKernelTask(TaskFunction_t fn, const char *name,
                      uint32_t stack, UBaseType_t prio)
{
    if (xTaskCreate(fn, name, stack, nullptr, prio, nullptr) != pdPASS)
    {
        Serial.printf("[KERNEL] ERRO criando task %s\n", name);
        return false;
    }
    return true;
}

void FatalRestart(const char *why)
{
    Serial.printf("[KERNEL] FATAL: %s - reiniciando\n", why);
    delay(2000);
    ESP.restart();
    while (true) { delay(1000); }
}

void PrintLocalRanks()
{
    Serial.printf("[KERNEL] Node %d executa os ranks: ", THIS_NODE_ID);

    bool first = true;
    int  count = 0;

    for (int rank = 0; rank < TOTAL_APPLICATION_TASKS; rank++)
    {
        if (!IsLocalRank(rank)) continue;

        if (!first) Serial.print(", ");
        Serial.print(rank);

        first = false;
        count++;
    }

    if (count == 0) Serial.print("(nenhum)");

    Serial.printf("  [%d de %d]\n", count, TOTAL_APPLICATION_TASKS);
}


// ============================================================
// CAMADA DE TRANSPORTE
// ============================================================

#if COMM_MODE == COMM_ESPNOW

// ---------------------- ESP-NOW ------------------------------

#define ESPNOW_TAG_DATA        0xD1
#define ESPNOW_TAG_CONTROL     0xC2
#define ESPNOW_TAG_DISCOVERY   0xE3

#define ESPNOW_MAX_FRAME       (1 + sizeof(Message))

static const uint8_t kBroadcastMac[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

typedef struct
{
    uint8_t  mac[6];
    uint16_t len;
    uint8_t  data[sizeof(Message)];
} EspNowRxPacket;

static QueueHandle_t espnowDataQ      = nullptr;
static QueueHandle_t espnowControlQ   = nullptr;
static QueueHandle_t espnowDiscoveryQ = nullptr;

static void EspNowHandleRx(const uint8_t *mac, const uint8_t *data, int len)
{
    if (mac == nullptr || data == nullptr) return;
    if (len < 2 || len > (int)ESPNOW_MAX_FRAME) return;

    QueueHandle_t q = nullptr;

    switch (data[0])
    {
    case ESPNOW_TAG_DATA:       q = espnowDataQ;      break;
    case ESPNOW_TAG_CONTROL:    q = espnowControlQ;   break;
    case ESPNOW_TAG_DISCOVERY:  q = espnowDiscoveryQ; break;
    default: return;
    }

    if (q == nullptr) return;

    EspNowRxPacket p;
    memcpy(p.mac, mac, 6);
    p.len = (uint16_t)(len - 1);
    memcpy(p.data, data + 1, len - 1);

    // Nunca bloqueia. Fila cheia => descarta e CONTA (perda por software).
    if (xQueueSend(q, &p, 0) != pdTRUE)
        espnowRxDrop++;
}

#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
static void EspNowRecvCb(const esp_now_recv_info_t *info,
                         const uint8_t *data, int len)
{
    if (info != nullptr) EspNowHandleRx(info->src_addr, data, len);
}
#else
static void EspNowRecvCb(const uint8_t *mac, const uint8_t *data, int len)
{
    EspNowHandleRx(mac, data, len);
}
#endif

// Callback de resultado de TX (nível MAC). Conta só unicast.
// A assinatura mudou no IDF 5.5 (recebe esp_now_send_info_t).
static void EspNowSendHandle(const uint8_t *destMac, esp_now_send_status_t status)
{
    if (destMac != nullptr && memcmp(destMac, kBroadcastMac, 6) == 0) return;

    if (status == ESP_NOW_SEND_SUCCESS) macTxOk++;
    else                                macTxFail++;
}

#if defined(ESP_IDF_VERSION) && defined(ESP_IDF_VERSION_VAL) && \
    (ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 5, 0))
static void EspNowSendCb(const esp_now_send_info_t *info, esp_now_send_status_t status)
{
    EspNowSendHandle(info ? info->des_addr : nullptr, status);
}
#else
static void EspNowSendCb(const uint8_t *mac, esp_now_send_status_t status)
{
    EspNowSendHandle(mac, status);
}
#endif

static bool EspNowEnsurePeer(const uint8_t *mac)
{
    if (esp_now_is_peer_exist(mac)) return true;

    esp_now_peer_info_t peer;
    memset(&peer, 0, sizeof(peer));
    memcpy(peer.peer_addr, mac, 6);
    peer.channel = 0;
    peer.ifidx   = WIFI_IF_STA;
    peer.encrypt = false;

    return esp_now_add_peer(&peer) == ESP_OK;
}

static bool EspNowSendRaw(const uint8_t *mac, uint8_t tag,
                          const void *payload, size_t len)
{
    if (len > sizeof(Message)) return false;

    uint8_t frame[ESPNOW_MAX_FRAME];
    frame[0] = tag;
    memcpy(frame + 1, payload, len);

    for (int i = 0; i < 5; i++)
    {
        esp_err_t r = esp_now_send(mac, frame, len + 1);

        if (r == ESP_OK) return true;
        if (r == ESP_ERR_ESPNOW_NOT_FOUND) return false;

        vTaskDelay(Ms(2));
    }
    return false;
}

static void Transport_Init()
{
    Serial.println("[COMM] Modo: ESP-NOW");

    espnowDataQ      = xQueueCreate(ESPNOW_RX_QUEUE_LENGTH, sizeof(EspNowRxPacket));
    espnowControlQ   = xQueueCreate(ESPNOW_RX_QUEUE_LENGTH, sizeof(EspNowRxPacket));
    espnowDiscoveryQ = xQueueCreate(8, sizeof(EspNowRxPacket));

    if (!espnowDataQ || !espnowControlQ || !espnowDiscoveryQ)
        FatalRestart("falha criando filas ESP-NOW");

    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.disconnect();

    esp_wifi_set_promiscuous(true);
    esp_wifi_set_channel(ESPNOW_CHANNEL, WIFI_SECOND_CHAN_NONE);
    esp_wifi_set_promiscuous(false);

    if (esp_now_init() != ESP_OK)
        FatalRestart("esp_now_init");

    esp_now_register_recv_cb(EspNowRecvCb);
    esp_now_register_send_cb(EspNowSendCb);

    if (!EspNowEnsurePeer(kBroadcastMac))
        FatalRestart("peer de broadcast ESP-NOW");

    Serial.printf("[COMM] MAC deste node: %s | canal %d\n",
                  WiFi.macAddress().c_str(), ESPNOW_CHANNEL);

    nodeDiscovered[THIS_NODE_ID] = true;
}

void LearnNode(int nodeId, const PeerAddr &addr)
{
    if (nodeId < 0 || nodeId >= NUM_NODES || nodeId == THIS_NODE_ID) return;

    bool changed = !nodeDiscovered[nodeId] ||
                   memcmp(nodeMAC[nodeId], addr.mac, 6) != 0;
    if (!changed) return;

    if (!EspNowEnsurePeer(addr.mac))
    {
        Serial.printf("[DISCOVERY] Falha adicionando peer do node %d\n", nodeId);
        return;
    }

    memcpy(nodeMAC[nodeId], addr.mac, 6);
    nodeDiscovered[nodeId] = true;

    Serial.printf("[DISCOVERY] Node %d: %02X:%02X:%02X:%02X:%02X:%02X\n", nodeId,
                  addr.mac[0], addr.mac[1], addr.mac[2],
                  addr.mac[3], addr.mac[4], addr.mac[5]);
}

static bool Transport_Recv(int channel, void *buf, size_t size,
                           PeerAddr &from, TickType_t wait)
{
    QueueHandle_t q = (channel == CH_DATA)    ? espnowDataQ
                    : (channel == CH_CONTROL) ? espnowControlQ
                                              : espnowDiscoveryQ;

    EspNowRxPacket p;
    if (xQueueReceive(q, &p, wait) != pdTRUE) return false;
    if (p.len != size) return false;

    memcpy(buf, p.data, size);
    memcpy(from.mac, p.mac, 6);
    return true;
}

bool SendControlMessage(int node, const ControlMessage &message)
{
    if (node < 0 || node >= NUM_NODES || node == THIS_NODE_ID) return false;
    if (!nodeDiscovered[node]) return false;

    xSemaphoreTake(controlTxMutex, portMAX_DELAY);
    bool ok = EspNowSendRaw(nodeMAC[node], ESPNOW_TAG_CONTROL,
                            &message, sizeof(ControlMessage));
    xSemaphoreGive(controlTxMutex);
    return ok;
}

bool SendDataPacket(int node, const Message &msg)
{
    if (node < 0 || node >= NUM_NODES || node == THIS_NODE_ID) return false;
    if (!nodeDiscovered[node]) return false;

    xSemaphoreTake(dataTxMutex, portMAX_DELAY);
    bool ok = EspNowSendRaw(nodeMAC[node], ESPNOW_TAG_DATA,
                            &msg, sizeof(Message));
    xSemaphoreGive(dataTxMutex);
    return ok;
}

void SendDiscovery()
{
    DiscoveryMessage msg;
    memset(&msg, 0, sizeof(msg));
    msg.magic  = DISCOVERY_MAGIC;
    msg.nodeId = THIS_NODE_ID;

    xSemaphoreTake(discoveryTxMutex, portMAX_DELAY);
    EspNowSendRaw(kBroadcastMac, ESPNOW_TAG_DISCOVERY, &msg, sizeof(msg));
    xSemaphoreGive(discoveryTxMutex);
}

#else  // COMM_MODE == COMM_WIFI

// ---------------------- WI-FI / UDP --------------------------

static void Transport_Init()
{
    Serial.println("[COMM] Modo: WiFi/UDP");

    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.setAutoReconnect(true);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    Serial.print("[WIFI] Conectando");

    uint32_t startWifi = millis();
    while (WiFi.status() != WL_CONNECTED)
    {
        delay(500);
        Serial.print(".");

        if (millis() - startWifi > 30000)
            FatalRestart("timeout do Wi-Fi");
    }

    Serial.println();
    Serial.printf("[WIFI] Conectado: %s\n", WiFi.localIP().toString().c_str());

    nodeIP[THIS_NODE_ID]         = WiFi.localIP();
    nodeDiscovered[THIS_NODE_ID] = true;

    if (!udpDataRx.begin(DATA_PORT))           FatalRestart("DATA_PORT");
    if (!udpDiscoveryRx.begin(DISCOVERY_PORT)) FatalRestart("DISCOVERY_PORT");
    if (!udpControlRx.begin(CONTROL_PORT))     FatalRestart("CONTROL_PORT");
}

void LearnNode(int nodeId, const PeerAddr &addr)
{
    if (nodeId < 0 || nodeId >= NUM_NODES || nodeId == THIS_NODE_ID) return;
    if (addr.ip == IPAddress(0, 0, 0, 0)) return;

    if (!nodeDiscovered[nodeId] || nodeIP[nodeId] != addr.ip)
    {
        nodeIP[nodeId] = addr.ip;
        nodeDiscovered[nodeId] = true;

        Serial.printf("[DISCOVERY] Node %d: %s\n",
                      nodeId, addr.ip.toString().c_str());
    }
}

static bool Transport_Recv(int channel, void *buf, size_t size,
                           PeerAddr &from, TickType_t wait)
{
    WiFiUDP &u = (channel == CH_DATA)    ? udpDataRx
               : (channel == CH_CONTROL) ? udpControlRx
                                         : udpDiscoveryRx;

    int packetSize = u.parsePacket();

    if (packetSize <= 0)
    {
        if (wait > 0) vTaskDelay(Ms(1));
        return false;
    }

    if (packetSize != (int)size)
    {
        while (u.available()) u.read();
        return false;
    }

    from.ip = u.remoteIP();

    int received = u.read((uint8_t *)buf, size);
    return received == (int)size;
}

bool SendControlMessage(int node, const ControlMessage &message)
{
    if (node < 0 || node >= NUM_NODES || node == THIS_NODE_ID) return false;
    if (!nodeDiscovered[node]) return false;

    xSemaphoreTake(controlTxMutex, portMAX_DELAY);

    bool ok = udpControlTx.beginPacket(nodeIP[node], CONTROL_PORT);

    if (ok)
    {
        udpControlTx.write((const uint8_t *)&message, sizeof(ControlMessage));
        ok = (udpControlTx.endPacket() == 1);
    }

    xSemaphoreGive(controlTxMutex);
    return ok;
}

bool SendDataPacket(int node, const Message &msg)
{
    if (node < 0 || node >= NUM_NODES || node == THIS_NODE_ID) return false;
    if (!nodeDiscovered[node]) return false;

    xSemaphoreTake(dataTxMutex, portMAX_DELAY);

    bool ok = udpDataTx.beginPacket(nodeIP[node], DATA_PORT);

    if (ok)
    {
        udpDataTx.write((const uint8_t *)&msg, sizeof(Message));
        ok = (udpDataTx.endPacket() == 1);
    }

    xSemaphoreGive(dataTxMutex);
    return ok;
}

static IPAddress BroadcastAddress()
{
    IPAddress ip   = WiFi.localIP();
    IPAddress mask = WiFi.subnetMask();

    return IPAddress(ip[0] | (uint8_t)~mask[0],
                     ip[1] | (uint8_t)~mask[1],
                     ip[2] | (uint8_t)~mask[2],
                     ip[3] | (uint8_t)~mask[3]);
}

void SendDiscovery()
{
    DiscoveryMessage msg;
    IPAddress ip = WiFi.localIP();

    msg.magic  = DISCOVERY_MAGIC;
    msg.nodeId = THIS_NODE_ID;
    msg.ip[0] = ip[0]; msg.ip[1] = ip[1];
    msg.ip[2] = ip[2]; msg.ip[3] = ip[3];

    xSemaphoreTake(discoveryTxMutex, portMAX_DELAY);

    if (udpDiscoveryTx.beginPacket(BroadcastAddress(), DISCOVERY_PORT))
    {
        udpDiscoveryTx.write((uint8_t *)&msg, sizeof(msg));
        udpDiscoveryTx.endPacket();
    }

    xSemaphoreGive(discoveryTxMutex);
}

#endif  // COMM_MODE


static ControlMessage MakeControl(uint8_t type, int32_t rank,
                                  int32_t generation, int32_t status)
{
    ControlMessage m;
    memset(&m, 0, sizeof(m));
    m.type       = type;
    m.srcNode    = THIS_NODE_ID;
    m.rank       = rank;
    m.generation = generation;
    m.status     = status;
    return m;
}


// ============================================================
// MEDIÇÃO DE PERDA DE PACOTES
// ============================================================

// Percentual de perda dado enviados/recebidos. Retorna -1 se não há
// base de cálculo (nada enviado). Valores levemente negativos (por
// timing da leitura) são cortados em 0.
static float LossPct(uint32_t sent, uint32_t received)
{
    if (sent == 0) return -1.0f;
    if (received >= sent) return 0.0f;
    return 100.0f * (float)(sent - received) / (float)sent;
}

static void PrintPct(const char *label, float pct)
{
    if (pct < 0) Serial.printf("[STATS]    %-34s n/d\n", label);
    else         Serial.printf("[STATS]    %-34s %.2f%%\n", label, pct);
}

void Stats_Reset()
{
    for (int n = 0; n < NUM_NODES; n++)
    {
        memset((void *)&linkStats[n], 0, sizeof(LinkStats));
        peerSnap[n].valid = false;
        peerRun[n].valid  = false;
        peerRunGen[n]     = -1;
    }

    espnowRxDrop = 0;
    macTxOk      = 0;
    macTxFail    = 0;
}

// Contadores e taxas que dependem só deste node.
void Stats_PrintLocal()
{
    Serial.println("---------------- LINK STATS (local) ----------------");

    for (int n = 0; n < NUM_NODES; n++)
    {
        if (n == THIS_NODE_ID) continue;

        LinkStats &s = linkStats[n];

        Serial.printf("[STATS] -> node %d | msgs=%lu tentativas=%lu acks=%lu "
                      "falhaTX=%lu perdidasDeVez=%lu\n",
                      n, (unsigned long)s.msgs, (unsigned long)s.txAttempts,
                      (unsigned long)s.ackOk, (unsigned long)s.txSendFail,
                      (unsigned long)s.msgFailed);

        Serial.printf("[STATS] <- node %d | rxData=%lu unico=%lu dup=%lu "
                      "filaCheia=%lu ackTx=%lu ackRx=%lu\n",
                      n, (unsigned long)s.rxData, (unsigned long)s.rxUnique,
                      (unsigned long)s.rxDup, (unsigned long)s.rxQueueFull,
                      (unsigned long)s.ackTx, (unsigned long)s.ackRx);

        PrintPct("perda ida+volta (por tentativa)", LossPct(s.txAttempts, s.ackOk));

        float retx = (s.txAttempts == 0) ? -1.0f
                   : 100.0f * (float)(s.txAttempts - s.msgs) / (float)s.txAttempts;
        PrintPct("retransmissoes / tentativas", retx);

        PrintPct("mensagens perdidas de vez", LossPct(s.msgs, s.msgs - s.msgFailed));

        if (s.rxData > 0)
            PrintPct("duplicatas / rxData (ACK perdido)",
                     100.0f * (float)s.rxDup / (float)s.rxData);
    }

#if COMM_MODE == COMM_ESPNOW
    Serial.printf("[STATS] ESP-NOW MAC unicast ok=%lu fail=%lu | rxQueueDrop=%lu\n",
                  (unsigned long)macTxOk, (unsigned long)macTxFail,
                  (unsigned long)espnowRxDrop);
    PrintPct("falha MAC (apos retries do HW)", LossPct(macTxOk + macTxFail, macTxOk));
#endif
}

// Envia ao node 'n' um snapshot (acumulado) da visão local do link com ele.
static void Stats_SendSnapshot(int n)
{
    LinkStats &s = linkStats[n];

    ControlMessage m = MakeControl(CONTROL_STATS, -1, 0, 0);
    m.stat[0] = s.txAttempts;   // dados que enviei a ele
    m.stat[1] = s.rxData;       // dados que recebi dele
    m.stat[2] = s.ackTx;        // ACKs que enviei a ele
    m.stat[3] = s.ackRx;        // ACKs que recebi dele

    SendControlMessage(n, m);
}

// Chamado pelo TaskControl ao receber CONTROL_STATS.
//   generation == 0 : snapshot acumulado (Stats_ExchangeAndPrint)
//   generation  > 0 : snapshot da execução 'generation' do benchmark
static void Stats_StorePeerSnapshot(const ControlMessage &m)
{
    int n = m.srcNode;
    if (n < 0 || n >= NUM_NODES || n == THIS_NODE_ID) return;

    if (m.generation == 0)
    {
        peerSnap[n].txAttempts = m.stat[0];
        peerSnap[n].rxData     = m.stat[1];
        peerSnap[n].ackTx      = m.stat[2];
        peerSnap[n].ackRx      = m.stat[3];
        peerSnap[n].valid      = true;
    }
    else
    {
        peerRun[n].valid      = false;
        peerRun[n].txAttempts = m.stat[0];
        peerRun[n].rxData     = m.stat[1];
        peerRun[n].ackTx      = m.stat[2];
        peerRun[n].ackRx      = m.stat[3];
        peerRunGen[n]         = m.generation;
        peerRun[n].valid      = true;
    }
}

// Troca de snapshots ACUMULADOS (desde o Stats_Reset) e relatório final.
// Deve ser chamado por UMA task por node, com o tráfego já parado.
void Stats_ExchangeAndPrint()
{
#if SINGLE_NODE_MODE
    Serial.println("[STATS] SINGLE_NODE_MODE: sem rede, nada a medir");
    return;
#else
    Serial.println("[STATS] Trocando snapshots acumulados com os vizinhos...");

    uint32_t t0 = millis();
    while (millis() - t0 < STATS_EXCHANGE_MS)
    {
        for (int n = 0; n < NUM_NODES; n++)
            if (n != THIS_NODE_ID) Stats_SendSnapshot(n);

        vTaskDelay(Ms(STATS_EXCHANGE_PERIOD_MS));
    }

    Stats_PrintLocal();

    Serial.println("------- LINK STATS ACUMULADO DO BENCHMARK (ida/volta) -------");
    Serial.println("[CSV] node,peer,msgs,tent,ackOk,msgFalha,rxData,dup,"
                   "filaCheia,perdaDados_pct,perdaAck_pct,perdaRT_pct");

    for (int n = 0; n < NUM_NODES; n++)
    {
        if (n == THIS_NODE_ID) continue;

        LinkStats &s = linkStats[n];
        PeerSnapshot &p = peerSnap[n];

        Serial.printf("[STATS] ===== link node %d <-> node %d =====\n", THIS_NODE_ID, n);

        float lossRT = LossPct(s.txAttempts, s.ackOk);
        float lossData = -1, lossAck = -1;

        if (!p.valid)
        {
            Serial.printf("[STATS]    snapshot do node %d NAO recebido "
                          "(so a medicao local ida+volta vale)\n", n);
        }
        else
        {
            lossData = LossPct(s.txAttempts, p.rxData);
            lossAck  = LossPct(p.ackTx, s.ackRx);

            Serial.printf("[STATS]  %d -> %d  (dados %d->%d, ACK %d->%d)\n",
                          THIS_NODE_ID, n, THIS_NODE_ID, n, n, THIS_NODE_ID);
            PrintPct("perda de DADOS (ida)", lossData);
            PrintPct("perda de ACKs (volta)", lossAck);

            Serial.printf("[STATS]  %d -> %d  (dados %d->%d, ACK %d->%d)\n",
                          n, THIS_NODE_ID, n, THIS_NODE_ID, THIS_NODE_ID, n);
            PrintPct("perda de DADOS (ida)", LossPct(p.txAttempts, s.rxData));
            PrintPct("perda de ACKs (volta)", LossPct(s.ackTx, p.ackRx));
        }

        Serial.printf("[CSV] %d,%d,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%.3f,%.3f,%.3f\n",
                      THIS_NODE_ID, n,
                      (unsigned long)s.msgs, (unsigned long)s.txAttempts,
                      (unsigned long)s.ackOk, (unsigned long)s.msgFailed,
                      (unsigned long)s.rxData, (unsigned long)s.rxDup,
                      (unsigned long)s.rxQueueFull,
                      lossData, lossAck, lossRT);
    }

    Serial.println("[STATS] Interpretacao: rxDup alto => ACKs se perdendo (ou "
                   "DATA_ACK_TIMEOUT_MS curto). filaCheia/rxQueueDrop => perda "
                   "por software (consumo lento), nao por radio.");
#endif
}


// ============================================================
// INTERFACE MPI
// ============================================================

int MPI_Comm_rank()
{
    TaskContext *c = GetCurrentTaskContext();
    return c ? c->rank : -1;
}

int MPI_Comm_size()
{
    return TOTAL_APPLICATION_TASKS;
}

double MPI_Wtime()
{
    return (double)esp_timer_get_time() / 1000000.0;
}

double MPI_Wtime_micro()
{
    return esp_timer_get_time();
}

static bool CheckNotFinalized(TaskContext *context, const char *fname)
{
    if (context->finalized)
    {
        Serial.printf("[MPI] ERRO: %s chamado APOS MPI_Finalize() no rank %d\n",
                      fname, context->rank);
        return false;
    }
    return true;
}


// ============================================================
// MPI_Send
// ============================================================

bool MPI_Send(const void *buffer, int count, int destinationRank)
{
    TaskContext *context = GetCurrentTaskContext();

    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Send: contexto inexistente");
        return false;
    }

    if (!CheckNotFinalized(context, "MPI_Send")) return false;

    if (destinationRank < 0 || destinationRank >= TOTAL_APPLICATION_TASKS)
    {
        Serial.printf("[MPI] MPI_Send: destino invalido %d\n", destinationRank);
        return false;
    }

    if (count < 0 || count > MESSAGE_PAYLOAD || (count > 0 && buffer == nullptr))
    {
        Serial.printf("[MPI] MPI_Send: tamanho invalido %d\n", count);
        return false;
    }

    Message msg;
    memset(&msg, 0, sizeof(msg));

    msg.type     = MSG_APPLICATION;
    msg.srcRank  = context->rank;
    msg.dstRank  = destinationRank;
    msg.sequence = NextMessageSequence();
    msg.length   = count;

    if (count > 0) memcpy(msg.payload, buffer, count);

    if (xQueueSend(senderQueue, &msg, portMAX_DELAY) != pdTRUE)
    {
        Serial.println("[MPI] Erro ao enfileirar no senderQueue");
        return false;
    }

    return true;
}


// ============================================================
// MPI_Recv
// ============================================================

static int CopyOutMessage(TaskContext *context, const Message &msg,
                          void *buffer, int maxCount)
{
    int copySize = msg.length;

    if (copySize > maxCount) copySize = maxCount;
    if (copySize > MESSAGE_PAYLOAD) copySize = MESSAGE_PAYLOAD;
    if (copySize > 0 && buffer != nullptr)
        memcpy(buffer, msg.payload, copySize);

    return copySize;
}

int MPI_Recv(void *buffer, int maxCount, int sourceRank)
{
    TaskContext *context = GetCurrentTaskContext();

    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Recv: contexto inexistente");
        return -1;
    }

    if (!CheckNotFinalized(context, "MPI_Recv")) return -1;

    for (int i = 0; i < context->pendingCount; i++)
    {
        if (sourceRank < 0 || context->pending[i].srcRank == sourceRank)
        {
            Message msg = context->pending[i];

            for (int j = i + 1; j < context->pendingCount; j++)
                context->pending[j - 1] = context->pending[j];

            context->pendingCount--;

            return CopyOutMessage(context, msg, buffer, maxCount);
        }
    }

    Message msg;

    while (true)
    {
        if (xQueueReceive(context->receiveQueue, &msg, portMAX_DELAY) != pdTRUE)
            continue;

        if (sourceRank < 0 || msg.srcRank == sourceRank)
            return CopyOutMessage(context, msg, buffer, maxCount);

        if (context->pendingCount < PENDING_MAX)
        {
            context->pending[context->pendingCount++] = msg;
        }
        else
        {
            Serial.printf("[MPI] Rank %d: buffer de pendentes cheio, "
                          "descartando msg de %d\n",
                          context->rank, (int)msg.srcRank);
        }
    }
}


// ============================================================
// BARREIRA
// ============================================================

static void SendBarrierRelease(int node, int32_t generation)
{
    if (node < 0 || node >= NUM_NODES || node == THIS_NODE_ID) return;
    if (!nodeDiscovered[node]) return;

    ControlMessage rel = MakeControl(CONTROL_BARRIER_RELEASE, -1, generation, 1);
    SendControlMessage(node, rel);
}

static void Barrier_ReleaseLocal(int32_t generation)
{
    xSemaphoreTake(barrierMutex, portMAX_DELAY);

    for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
    {
        if (IsLocalRank(i) && barrierWaitGen[i] == generation)
        {
            barrierWaitGen[i] = -1;

            if (taskContexts[i].barrierSem != nullptr)
                xSemaphoreGive(taskContexts[i].barrierSem);
        }
    }

    xSemaphoreGive(barrierMutex);
}

static void Barrier_HandleArrival(int rank, int32_t generation)
{
    if (rank < 0 || rank >= TOTAL_APPLICATION_TASKS) return;

    bool    completed = false;
    bool    stale     = false;
    int32_t releaseGen = generation;

    xSemaphoreTake(barrierMutex, portMAX_DELAY);

    if (generation < coordGeneration)
    {
        stale = true;
    }
    else if (generation == coordGeneration)
    {
        barrierArrivedGeneration[rank] = generation;

        int count = 0;
        for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
            if (barrierArrivedGeneration[i] == generation) count++;

#if KERNEL_VERBOSE
        Serial.printf("[BARRIER] Rank %d chegou, geracao %ld (%d/%d)\n",
                      rank, (long)generation, count, TOTAL_APPLICATION_TASKS);
#endif

        if (count == TOTAL_APPLICATION_TASKS)
        {
            completed = true;
            releaseGen = generation;
            coordGeneration = generation + 1;
        }
    }

    xSemaphoreGive(barrierMutex);

    if (completed)
    {
#if KERNEL_VERBOSE
        Serial.printf("[BARRIER] Geracao %ld completa - liberando\n",
                      (long)releaseGen);
#endif

        Barrier_ReleaseLocal(releaseGen);

        for (int n = 0; n < NUM_NODES; n++)
            if (n != THIS_NODE_ID) SendBarrierRelease(n, releaseGen);
    }
    else if (stale && !IsLocalRank(rank))
    {
        SendBarrierRelease(RankToNode(rank), generation);
    }
}

void MPI_Barrier()
{
    TaskContext *context = GetCurrentTaskContext();

    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Barrier: contexto inexistente");
        return;
    }

    if (!CheckNotFinalized(context, "MPI_Barrier")) return;

    int     rank       = context->rank;
    int32_t generation = context->barrierGen++;

    xSemaphoreTake(barrierMutex, portMAX_DELAY);
    barrierWaitGen[rank] = generation;
    xSemaphoreGive(barrierMutex);

    if (THIS_NODE_ID == 0)
    {
        Barrier_HandleArrival(rank, generation);
        xSemaphoreTake(context->barrierSem, portMAX_DELAY);
    }
    else
    {
        while (true)
        {
            if (nodeDiscovered[0])
            {
                ControlMessage m = MakeControl(CONTROL_BARRIER, rank, generation, 0);
                SendControlMessage(0, m);
            }

            if (xSemaphoreTake(context->barrierSem, Ms(BARRIER_RESEND_MS)) == pdTRUE)
                break;
        }
    }

#if KERNEL_VERBOSE
    Serial.printf("[BARRIER] Rank %d liberado da geracao %ld\n",
                  rank, (long)generation);
#endif
}


// ============================================================
// BROADCAST
// ============================================================

bool MPI_Bcast(void *buffer, int count, int rootRank)
{
    TaskContext *context = GetCurrentTaskContext();

    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Bcast: contexto inexistente");
        return false;
    }

    if (!CheckNotFinalized(context, "MPI_Bcast")) return false;

    if (rootRank < 0 || rootRank >= TOTAL_APPLICATION_TASKS)
    {
        Serial.printf("[MPI] MPI_Bcast: raiz invalida %d\n", rootRank);
        return false;
    }

    if (count < 0 || count > MESSAGE_PAYLOAD || (count > 0 && buffer == nullptr))
    {
        Serial.printf("[MPI] MPI_Bcast: tamanho invalido %d\n", count);
        return false;
    }

    int rank = context->rank;

    if (rank == rootRank)
    {
        bool allOk = true;

        for (int dst = 0; dst < TOTAL_APPLICATION_TASKS; dst++)
        {
            if (dst == rootRank) continue;

            if (!MPI_Send(buffer, count, dst))
            {
                Serial.printf("[MPI] MPI_Bcast: falha enviando rank %d -> %d\n",
                              rootRank, dst);
                allOk = false;
            }
        }

        return allOk;
    }

    int received = MPI_Recv(buffer, count, rootRank);
    return received >= 0;
}


// ============================================================
// FINALIZAÇÃO
// ============================================================

bool MPI_Finalize()
{
    TaskContext *context = GetCurrentTaskContext();

    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Finalize: contexto inexistente");
        return false;
    }

    if (context->finalized)
    {
        Serial.printf("[MPI] MPI_Finalize: rank %d ja estava finalizado "
                      "(chamada ignorada)\n", context->rank);
        return true;
    }

    context->finalized = true;

    Serial.printf("[MPI] Rank %d: MPI_Finalize() concluido\n", context->rank);
    return true;
}

bool MPI_Finalized()
{
    TaskContext *context = GetCurrentTaskContext();
    return (context != nullptr) && context->finalized;
}


// ============================================================
// CRIAÇÃO / START DAS APPLICATION TASKS
// ============================================================

static void KernelTaskEntry(void *parameter)
{
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

    ApplicationTask(parameter);

    vTaskDelete(NULL);
}

bool Kernel_CreateApplicationTask(int rank)
{
    if (!IsLocalRank(rank)) return false;

    TaskContext *context = &taskContexts[rank];

    if (context->taskHandle != nullptr) return true;

    Serial.printf("[KERNEL] Criando rank %d...\n", rank);

    context->rank         = rank;
    context->barrierGen   = 0;
    context->pendingCount = 0;
    context->finalized    = false;

    // O rank 0 recebe as somas parciais de TODOS os workers em rajada (enquanto
    // ainda esta ocupado) -> a fila dele precisa comportar TOTAL-1 mensagens.
    int queueLen = QUEUE_LENGTH;
    if (rank == 0 && queueLen < TOTAL_APPLICATION_TASKS) queueLen = TOTAL_APPLICATION_TASKS;

    context->receiveQueue = xQueueCreate(queueLen, sizeof(Message));
    context->barrierSem   = xSemaphoreCreateBinary();
    context->pending      = (Message *)malloc(sizeof(Message) * PENDING_MAX);

    if (!context->receiveQueue || !context->barrierSem || !context->pending)
    {
        Serial.printf("[KERNEL] ERRO alocando recursos do rank %d\n", rank);
        goto fail;
    }

    {
        char taskName[16];
        snprintf(taskName, sizeof(taskName), "APP_%d", rank);

        uint32_t stackSize = (rank < NUM_NODES) ? APP_STACK_SIZE : APP_STACK_SIZE_SMALL;

        if (xTaskCreate(KernelTaskEntry, taskName, stackSize,
                        context, TASK_PRIORITY,
                        &context->taskHandle) != pdPASS)
        {
            Serial.printf("[KERNEL] ERRO ao criar rank %d\n", rank);
            context->taskHandle = nullptr;
            goto fail;
        }
    }

    Serial.printf("[KERNEL] Rank %d criado - aguardando START\n", rank);
    return true;

fail:
    if (context->receiveQueue) { vQueueDelete(context->receiveQueue); context->receiveQueue = nullptr; }
    if (context->barrierSem)   { vSemaphoreDelete(context->barrierSem); context->barrierSem = nullptr; }
    if (context->pending)      { free(context->pending); context->pending = nullptr; }
    return false;
}

void Kernel_StartLocalTasks()
{
    if (kernelReady) return;

    kernelReady = true;

    Serial.println();
    Serial.println("[KERNEL] ================================");
    Serial.println("[KERNEL] KERNEL READY - liberando tasks");
    Serial.println("[KERNEL] ================================");

    for (int rank = 0; rank < TOTAL_APPLICATION_TASKS; rank++)
    {
        if (IsLocalRank(rank) && taskContexts[rank].taskHandle != nullptr)
        {
            Serial.printf("[KERNEL] Liberando rank %d\n", rank);
            xTaskNotifyGive(taskContexts[rank].taskHandle);
        }
    }
}


// ============================================================
// TASK SENDER  (com contagem de tentativas / ACKs)
// ============================================================

void TaskSender(void *parameter)
{
    Message msg;

    while (true)
    {
        if (xQueueReceive(senderQueue, &msg, portMAX_DELAY) != pdTRUE)
            continue;

        int destinationNode = RankToNode(msg.dstRank);

        // ---------------- LOCAL ----------------
        if (destinationNode == THIS_NODE_ID)
        {
            TaskContext *dst = GetTaskContext(msg.dstRank);

            if (dst == nullptr || dst->receiveQueue == nullptr)
            {
                Serial.printf("[KERNEL] Falha entrega local %d -> %d: "
                              "rank de destino invalido\n",
                              (int)msg.srcRank, (int)msg.dstRank);
                continue;
            }

            bool delivered = false;

            for (int attempt = 0; attempt < DATA_MAX_RETRIES && !delivered; attempt++)
            {
                if (xQueueSend(dst->receiveQueue, &msg, Ms(1000)) == pdTRUE)
                {
                    delivered = true;
                }
                else
                {
                    Serial.printf("[KERNEL] Fila local do rank %d cheia - "
                                  "tentativa %d/%d (seq %lu)\n",
                                  (int)msg.dstRank, attempt + 1, DATA_MAX_RETRIES,
                                  (unsigned long)msg.sequence);
                }
            }

            if (!delivered)
            {
                Serial.printf("[KERNEL] ERRO: falha DEFINITIVA na entrega "
                              "local %d -> %d (seq %lu) apos %d tentativas\n",
                              (int)msg.srcRank, (int)msg.dstRank,
                              (unsigned long)msg.sequence, DATA_MAX_RETRIES);
            }

            continue;
        }

        // ---------------- REMOTO ----------------
        LinkStats &ls = linkStats[destinationNode];
        ls.msgs++;

        bool acked = false;

        for (int attempt = 0; attempt < DATA_MAX_RETRIES && !acked; attempt++)
        {
            if (!nodeDiscovered[destinationNode])
            {
                vTaskDelay(Ms(100));
                continue;
            }

            while (xSemaphoreTake(dataAckSemaphore, 0) == pdTRUE) {}

            if (!SendDataPacket(destinationNode, msg))
            {
                ls.txSendFail++;          // falha LOCAL, nao e perda no ar
                vTaskDelay(Ms(10));
                continue;
            }

            ls.txAttempts++;              // tentativa que realmente saiu

            TickType_t start   = xTaskGetTickCount();
            TickType_t timeout = Ms(DATA_ACK_TIMEOUT_MS);

            while (!acked)
            {
                TickType_t elapsed = xTaskGetTickCount() - start;
                if (elapsed >= timeout) break;

                if (xSemaphoreTake(dataAckSemaphore, timeout - elapsed) == pdTRUE)
                {
                    if (lastAckSeq == msg.sequence)
                    {
                        acked = true;
                        ls.ackOk++;
                    }
                }
            }
        }

        if (!acked)
        {
            ls.msgFailed++;
            Serial.printf("[KERNEL] ERRO: sem ACK de %d -> %d (seq %lu)\n",
                          (int)msg.srcRank, (int)msg.dstRank,
                          (unsigned long)msg.sequence);
        }
    }
}


// ============================================================
// TASK RECEIVER  (com contagem de recebidos / duplicatas / ACKs)
// ============================================================

void TaskReceiver(void *parameter)
{
    while (true)
    {
        Message  msg;
        PeerAddr from;

        if (!Transport_Recv(CH_DATA, &msg, sizeof(Message), from, Ms(10)))
            continue;

        // ---------- ACK de dados ----------
        if (msg.type == MSG_DATA_ACK)
        {
            int ackNode = RankToNode(msg.srcRank);
            if (ackNode >= 0 && ackNode < NUM_NODES && ackNode != THIS_NODE_ID)
                linkStats[ackNode].ackRx++;

            lastAckSeq = msg.sequence;
            xSemaphoreGive(dataAckSemaphore);
            continue;
        }

        // ---------- mensagem de aplicação ----------
        if (msg.type != MSG_APPLICATION) continue;

        if (msg.srcRank < 0 || msg.srcRank >= TOTAL_APPLICATION_TASKS) continue;
        if (!IsLocalRank(msg.dstRank)) continue;
        if (msg.length > MESSAGE_PAYLOAD) continue;

        int srcNode = RankToNode(msg.srcRank);
        if (srcNode == THIS_NODE_ID) continue;

        LearnNode(srcNode, from);

        LinkStats &ls = linkStats[srcNode];
        ls.rxData++;                                  // chegou pelo ar

        TaskContext *dst = GetTaskContext(msg.dstRank);

        if (dst == nullptr || dst->receiveQueue == nullptr) continue;

        bool duplicate = (lastDeliveredSeq[srcNode] == (int64_t)msg.sequence);
        bool delivered = duplicate;

        if (duplicate)
        {
            ls.rxDup++;                               // ACK anterior se perdeu
        }
        else
        {
            delivered = (xQueueSend(dst->receiveQueue, &msg, Ms(50)) == pdTRUE);

            if (delivered)
            {
                lastDeliveredSeq[srcNode] = (int64_t)msg.sequence;
                ls.rxUnique++;
            }
            else
            {
                ls.rxQueueFull++;
                Serial.printf("[KERNEL] Fila do rank %d cheia - sem ACK\n",
                              (int)msg.dstRank);
            }
        }

        if (delivered)
        {
            Message ack;
            memset(&ack, 0, sizeof(ack));
            ack.type     = MSG_DATA_ACK;
            ack.srcRank  = msg.dstRank;
            ack.dstRank  = msg.srcRank;
            ack.sequence = msg.sequence;

            SendDataPacket(srcNode, ack);
            ls.ackTx++;
        }
    }
}


// ============================================================
// DISCOVERY
// ============================================================

void TaskDiscovery(void *parameter)
{
    Serial.println("[DISCOVERY] Task iniciada");

    uint32_t lastSend = 0;
    bool reportedAll  = false;

    while (true)
    {
        bool allFound = true;
        for (int i = 0; i < NUM_NODES; i++)
            if (!nodeDiscovered[i]) { allFound = false; break; }

        uint32_t interval = allFound ? 5000 : 1000;
        uint32_t now = millis();

        if (now - lastSend >= interval)
        {
            SendDiscovery();
            lastSend = now;
        }

        DiscoveryMessage msg;
        PeerAddr from;

        if (Transport_Recv(CH_DISCOVERY, &msg, sizeof(msg), from, 0))
        {
            if (msg.magic == DISCOVERY_MAGIC)
                LearnNode(msg.nodeId, from);
        }

        if (allFound && !reportedAll)
        {
            reportedAll = true;
            Serial.println("[DISCOVERY] Todos os nodes encontrados");
        }

        vTaskDelay(Ms(10));
    }
}


// ============================================================
// COMANDOS DO MASTER
// ============================================================

bool SendCreateTaskCommand(int rank)
{
    int destinationNode = RankToNode(rank);

    if (destinationNode == THIS_NODE_ID)
        return Kernel_CreateApplicationTask(rank);

    for (int attempt = 0; attempt < CONTROL_MAX_RETRIES; attempt++)
    {
        if (!nodeDiscovered[destinationNode])
        {
            vTaskDelay(Ms(500));
            continue;
        }

        while (xSemaphoreTake(createAckSemaphore, 0) == pdTRUE) {}

        lastCreateAckRank   = -1;
        lastCreateAckStatus = 0;

        ControlMessage msg = MakeControl(CONTROL_CREATE_TASK, rank, 0, 0);

        if (!SendControlMessage(destinationNode, msg))
        {
            vTaskDelay(Ms(100));
            continue;
        }

        TickType_t start   = xTaskGetTickCount();
        TickType_t timeout = Ms(CONTROL_ACK_TIMEOUT_MS);

        while (true)
        {
            TickType_t elapsed = xTaskGetTickCount() - start;
            if (elapsed >= timeout) break;

            if (xSemaphoreTake(createAckSemaphore, timeout - elapsed) == pdTRUE)
            {
                if (lastCreateAckRank == rank)
                {
                    if (lastCreateAckStatus == 1)
                    {
                        Serial.printf("[MASTER] Rank %d confirmado pelo node %d\n",
                                      rank, destinationNode);
                        return true;
                    }
                    return false;
                }
            }
        }

        Serial.printf("[MASTER] Timeout CREATE rank %d (tentativa %d)\n",
                      rank, attempt + 1);
    }

    return false;
}

bool SendStartCommand(int destinationNode)
{
    for (int attempt = 0; attempt < CONTROL_MAX_RETRIES; attempt++)
    {
        if (!nodeDiscovered[destinationNode])
        {
            vTaskDelay(Ms(500));
            continue;
        }

        while (xSemaphoreTake(startAckSemaphore, 0) == pdTRUE) {}
        lastStartAckStatus = 0;

        Serial.printf("[MASTER] Enviando START ao node %d\n", destinationNode);

        ControlMessage msg = MakeControl(CONTROL_START, -1, 0, 0);

        if (!SendControlMessage(destinationNode, msg))
        {
            vTaskDelay(Ms(100));
            continue;
        }

        if (xSemaphoreTake(startAckSemaphore, Ms(CONTROL_ACK_TIMEOUT_MS)) == pdTRUE)
            return (lastStartAckStatus == 1);
    }

    return false;
}


// ============================================================
// TASK MASTER (somente Node 0)
// ============================================================

void TaskMaster(void *parameter)
{
    Serial.println("\n[MASTER] Aguardando discovery dos outros nodes...");

    for (int n = 0; n < NUM_NODES; n++)
    {
        while (!nodeDiscovered[n])
            vTaskDelay(Ms(500));
    }

    Serial.println("[MASTER] Todos os nodes encontrados");
    Serial.println("[MASTER] CRIANDO APPLICATION TASKS");

    for (int rank = 0; rank < TOTAL_APPLICATION_TASKS; rank++)
    {
        Serial.printf("[MASTER] Criando rank %d\n", rank);

        if (!SendCreateTaskCommand(rank))
        {
            Serial.printf("[MASTER] ERRO criando rank %d\n", rank);
            vTaskDelete(NULL);
            return;
        }

        vTaskDelay(Ms(50));
    }

    Serial.println("[MASTER] TODAS AS TASKS FORAM CRIADAS");

    for (int n = 0; n < NUM_NODES; n++)
    {
        if (n == THIS_NODE_ID) continue;

        if (!SendStartCommand(n))
        {
            Serial.printf("[MASTER] ERRO no START do node %d\n", n);
            vTaskDelete(NULL);
            return;
        }

        Serial.printf("[MASTER] Node %d confirmou START\n", n);
    }

    Kernel_StartLocalTasks();

    Serial.println("[MASTER] SISTEMA DISTRIBUIDO PRONTO");

    vTaskDelete(NULL);
}


// ============================================================
// TASK CONTROL
// ============================================================

void TaskControl(void *parameter)
{
    Serial.printf("[CONTROL] Task iniciada no node %d\n", THIS_NODE_ID);

    while (true)
    {
        ControlMessage msg;
        PeerAddr from;

        if (!Transport_Recv(CH_CONTROL, &msg, sizeof(ControlMessage), from, Ms(10)))
            continue;

        LearnNode(msg.srcNode, from);

        switch (msg.type)
        {
        case CONTROL_CREATE_TASK:
        {
            Serial.printf("[CONTROL] CREATE rank %d recebido\n", (int)msg.rank);

            bool ok = Kernel_CreateApplicationTask(msg.rank);

            ControlMessage ack = MakeControl(CONTROL_CREATE_ACK, msg.rank, 0, ok ? 1 : 0);
            SendControlMessage(msg.srcNode, ack);
            break;
        }

        case CONTROL_CREATE_ACK:
            lastCreateAckRank   = msg.rank;
            lastCreateAckStatus = msg.status;
            xSemaphoreGive(createAckSemaphore);
            break;

        case CONTROL_START:
        {
            Serial.println("[CONTROL] START recebido");

            Kernel_StartLocalTasks();

            ControlMessage ack = MakeControl(CONTROL_START_ACK, -1, 0, 1);
            SendControlMessage(msg.srcNode, ack);
            break;
        }

        case CONTROL_START_ACK:
            lastStartAckStatus = msg.status;
            xSemaphoreGive(startAckSemaphore);
            break;

        case CONTROL_BARRIER:
            if (THIS_NODE_ID == 0)
                Barrier_HandleArrival(msg.rank, msg.generation);
            break;

        case CONTROL_BARRIER_RELEASE:
            if (THIS_NODE_ID != 0)
            {
#if KERNEL_VERBOSE
                Serial.printf("[BARRIER] RELEASE geracao %ld recebido\n",
                              (long)msg.generation);
#endif
                Barrier_ReleaseLocal(msg.generation);
            }
            break;

        case CONTROL_STATS:
            Stats_StorePeerSnapshot(msg);
            break;

        default:
            break;
        }
    }
}


// ============================================================
// ============================================================
// A PARTIR DAQUI COMEÇA A APLICAÇÃO
//   BENCHMARK 2: SOMA DE UM VETOR
// ============================================================
// ============================================================

// ---------------- Parâmetros do benchmark -------------------

#ifndef BENCH_REPS
#define BENCH_REPS               1          // repetições por número de ranks
#endif

#define BENCH_VECTOR_SIZE        100000     // tamanho fixo do vetor

#ifndef BENCH_PRINT_RANKS
#define BENCH_PRINT_RANKS        0          // 1 = imprime Tcalc/Tcomm de cada rank
#endif

#ifndef BENCH_PRINT_RETX_PER_RUN
#define BENCH_PRINT_RETX_PER_RUN 1          // 1 = linha [RETX] por execução/vizinho
#endif

#define BENCH_SETTLE_MS          300        // espera após a barreira final

static const int kRanks[] = {1, 2, 4, 8, 16, 32};

#define BENCH_N_RANKS   ((int)(sizeof(kRanks) / sizeof(kRanks[0])))

#define BENCH_CHUNK_ELEMS   (MESSAGE_PAYLOAD / (int)sizeof(int16_t))   // 64 int16 por mensagem
#define BENCH_BLOCK_CHUNKS  16
#define BENCH_BLOCK_ELEMS   (BENCH_CHUNK_ELEMS * BENCH_BLOCK_CHUNKS)   // 1024 elementos

#if COMM_MODE == COMM_ESPNOW
#define COMM_NAME "ESPNOW"
#else
#define COMM_NAME "UDP"
#endif


// ---------------- Utilidades --------------------------------

static inline int64_t NowUs() { return esp_timer_get_time(); }
static inline double  Pos(double v) { return v > 0.0 ? v : 0.0; }

// Calibra o custo de ler o timer (descontado de cada janela medida).
void Bench_CalibrateTimer()
{
    const int K = 4000;
    int64_t acc = 0;

    for (int i = 0; i < K; i++)
    {
        int64_t a = esp_timer_get_time();
        int64_t b = esp_timer_get_time();
        acc += (b - a);
    }

    gTimerOvhUs = (double)acc / (double)K;
}

static int16_t Bench_GenerateValue(int index, uint32_t seed)
{
    uint32_t x = ((uint32_t)index + 1u) * 2654435761u;

    x ^= seed * 2246822519u;

    x ^= x >> 16;
    x *= 3266489917u;
    x ^= x >> 13;

    return (int16_t)((int32_t)(x % 60000u) - 30000);
}

// Fatia do rank 'rank' quando há R ranks ativos e N elementos.
static void Bench_Range(int rank, int R, int N, int *start, int *count)
{
    int base = N / R;
    int rem  = N % R;

    *count = base + (rank < rem ? 1 : 0);
    *start = rank * base + (rank < rem ? rank : rem);
}

static void Bench_GenBlock(int16_t *blk, int startIdx, int n, uint32_t seed, int64_t &ref)
{
    int16_t localMax = INT16_MIN;

    for (int i = 0; i < n; i++)
    {
        int16_t v = Bench_GenerateValue(startIdx + i, seed);
        blk[i] = v;
        if (v > localMax) localMax = v;
    }

    if (localMax > ref) ref = localMax;
}

// ---------------- Rank 0 ------------------------------------

static void Bench_Rank0(int N, int R, uint32_t seed, BenchRaw &o)
{
    int16_t *blk = (int16_t *)malloc(BENCH_BLOCK_ELEMS * sizeof(int16_t));
    if (blk == nullptr) FatalRestart("sem memoria para bloco (rank 0)");

    const double ovh = gTimerOvhUs;

    int64_t ref = INT16_MIN, own = INT16_MIN, global = INT16_MIN;
    double  genUs = 0, commUs = 0, calcUs = 0, redUs = 0;
    int     sendFail = 0;
    bool    ok = true;

    int64_t tStart = NowUs();

    // ---- distribuição: gera e envia a fatia de cada rank 1..R-1 ----
    for (int dst = 1; dst < R; dst++)
    {
        int s, c;
        Bench_Range(dst, R, N, &s, &c);

        int off = 0;
        while (off < c)
        {
            int n = c - off;
            if (n > BENCH_BLOCK_ELEMS) n = BENCH_BLOCK_ELEMS;

            int64_t g0 = NowUs();
            Bench_GenBlock(blk, s + off, n, seed, ref);
            genUs += (double)(NowUs() - g0) - ovh;

            int64_t c0 = NowUs();
            for (int k = 0; k < n; k += BENCH_CHUNK_ELEMS)
            {
                int m = n - k;
                if (m > BENCH_CHUNK_ELEMS) m = BENCH_CHUNK_ELEMS;

                if (!MPI_Send(blk + k, m * (int)sizeof(int16_t), dst)) sendFail++;
            }
            commUs += (double)(NowUs() - c0) - ovh;

            off += n;
        }
    }

    // ---- fatia do próprio rank 0 ----
    {
        int s, c;
        Bench_Range(0, R, N, &s, &c);

        int off = 0;
        while (off < c)
        {
            int n = c - off;
            if (n > BENCH_BLOCK_ELEMS) n = BENCH_BLOCK_ELEMS;

            int64_t g0 = NowUs();
            Bench_GenBlock(blk, s + off, n, seed, ref);
            genUs += (double)(NowUs() - g0) - ovh;

            int64_t k0 = NowUs();
            int16_t partMax = INT16_MIN;
            for (int i = 0; i < n; i++)
                if (blk[i] > partMax) partMax = blk[i];
            if (partMax > own) own = partMax;
            calcUs += (double)(NowUs() - k0) - ovh;

            off += n;
        }
    }

    global = own;

    memset(o.rankCalcUs, 0, sizeof(o.rankCalcUs));
    memset(o.rankCommUs, 0, sizeof(o.rankCommUs));
    o.rankCalcUs[0] = (uint32_t)Pos(calcUs);
    o.rankCommUs[0] = (uint32_t)Pos(commUs);

    // ---- redução: recebe o máximo parcial de qualquer worker (R-1 mensagens) ----
    int64_t r0 = NowUs();
    for (int k = 1; k < R; k++)
    {
        BenchPartial pm;
        int n = MPI_Recv(&pm, sizeof(pm), MPI_ANY_SOURCE);

        if (n != (int)sizeof(pm) || pm.rank < 1 || pm.rank >= R)
        {
            ok = false;
            continue;
        }

        if (pm.maxVal > global) global = pm.maxVal;
        o.rankCalcUs[pm.rank] = pm.calcUs;
        o.rankCommUs[pm.rank] = pm.commUs;
    }
    redUs = (R > 1) ? ((double)(NowUs() - r0) - ovh) : 0.0;

    o.totalUs  = (double)(NowUs() - tStart);
    o.genUs    = Pos(genUs);
    o.commUs   = Pos(commUs);
    o.calcUs   = Pos(calcUs);
    o.redUs    = Pos(redUs);
    o.global   = global;
    o.ref      = ref;
    o.sendFail = sendFail;
    o.ok       = ok && (sendFail == 0) && (global == ref);

    free(blk);
}

// ---------------- Workers (rank 1..R-1) ---------------------

static void Bench_Worker(int rank, int N, int R)
{
    int16_t *blk = (int16_t *)malloc(BENCH_BLOCK_ELEMS * sizeof(int16_t));
    if (blk == nullptr) FatalRestart("sem memoria para bloco (worker)");

    const double ovh = gTimerOvhUs;

    int s, c;
    Bench_Range(rank, R, N, &s, &c);

    int64_t localMax = INT16_MIN;
    double  commUs = 0, calcUs = 0;
    int     got = 0;
    bool    failed = false;

    while (got < c && !failed)
    {
        int fill = 0;

        // recebe até um bloco inteiro (ou o que falta da fatia)
        int64_t c0 = NowUs();
        while (fill < BENCH_BLOCK_ELEMS && got + fill < c)
        {
            int n = MPI_Recv(blk + fill,
                             (BENCH_BLOCK_ELEMS - fill) * (int)sizeof(int16_t), 0);
            if (n <= 0) { failed = true; break; }

            fill += n / (int)sizeof(int16_t);
        }
        commUs += (double)(NowUs() - c0) - ovh;

        int64_t k0 = NowUs();
        int16_t partMax = INT16_MIN;
        for (int i = 0; i < fill; i++)
            if (blk[i] > partMax) partMax = blk[i];
        if (partMax > localMax) localMax = partMax;
        calcUs += (double)(NowUs() - k0) - ovh;

        got += fill;
    }

    BenchPartial pm;
    pm.maxVal = localMax;
    pm.commUs = (uint32_t)Pos(commUs);
    pm.calcUs = (uint32_t)Pos(calcUs);
    pm.rank   = rank;

    MPI_Send(&pm, sizeof(pm), 0);

    free(blk);
}

// Uma execução completa para este rank. Só o rank 0 preenche 'raw'.
static void Bench_Run(int rank, int N, int R, int32_t runId, BenchRaw &raw)
{
    int64_t result = 0;

    if (rank >= R)
    {
        // inativo: não faz nada, só espera o MPI_Bcast abaixo
    }
    else if (rank == 0)
    {
        Bench_Rank0(N, R, (uint32_t)runId, raw);
        result = raw.global;
    }
    else
    {
        Bench_Worker(rank, N, R);
    }

    // Todos os ranks (ativos e inativos) recebem a soma global.
    // Fora da medição: serve de liberação para os ranks que esperam.
    MPI_Bcast(&result, sizeof(result), 0);
}

// ---------------- Retransmissões por execução ----------------

static void Bench_TakeBase(LinkStats *base)
{
    memcpy((void *)base, (const void *)linkStats, sizeof(LinkStats) * NUM_NODES);
}

// Retransmissões = tentativas de envio - mensagens (contadas só por este node,
// nenhuma troca de dados entre nodes é necessária).
static void Bench_RetxReport(int N, int R, int rep, const LinkStats *base, RetxAccum &acc)
{
#if SINGLE_NODE_MODE
    (void)N; (void)R; (void)rep; (void)base; (void)acc;
    return;
#else
    for (int n = 0; n < NUM_NODES; n++)
    {
        if (n == THIS_NODE_ID) continue;

        uint32_t msgs   = linkStats[n].msgs       - base[n].msgs;
        uint32_t tx     = linkStats[n].txAttempts - base[n].txAttempts;
        uint32_t failed = linkStats[n].msgFailed  - base[n].msgFailed;
        uint32_t retx   = (tx > msgs) ? (tx - msgs) : 0;

        acc.msgs += msgs;  acc.tx += tx;  acc.retx += retx;  acc.failed += failed;

#if BENCH_PRINT_RETX_PER_RUN
        if (msgs || tx)
        {
            Serial.printf("[RETX] node%d->node%d N=%d R=%d rep=%d | mensagens=%lu "
                          "tentativas=%lu retransmissoes=%lu msgPerdidaDeVez=%lu\n",
                          THIS_NODE_ID, n, N, R, rep + 1,
                          (unsigned long)msgs, (unsigned long)tx,
                          (unsigned long)retx, (unsigned long)failed);
        }
#endif
    }
#endif
}

// ---------------- Impressão ---------------------------------

#define BENCH_CSV_RES_HEADER \
    "[CSV_RES_HEADER] modo,nodes,N,ranks,reps,Ttotal_ms,Tgen_ms,Tcomm_ms,Tcalc0_ms," \
    "TcalcMax_ms,TcommMax_ms,Tred_ms,Speedup,Eficiencia,SpeedupCalc," \
    "mensagens,tentativas,retransmissoes,msgPerdidaDeVez,ok"

static void Bench_PrintHeader()
{
    Serial.println();
    Serial.println("==========================================================");
    Serial.println(" BENCHMARK: MÁXIMO DE UM VETOR  (cluster ESP32 MPI)");
    Serial.println("==========================================================");
    Serial.printf("[BENCH] Modo=%s  nodes=%d  ranks_totais=%d  payload=%d B (%d int16/msg)\n",
                  COMM_NAME, NUM_NODES, TOTAL_APPLICATION_TASKS,
                  MESSAGE_PAYLOAD, BENCH_CHUNK_ELEMS);
    Serial.printf("[BENCH] Vetor fixo: N=%d elementos\n", BENCH_VECTOR_SIZE);

    Serial.print("[BENCH] Ranks R:");
    for (int i = 0; i < BENCH_N_RANKS; i++)
        if (kRanks[i] <= TOTAL_APPLICATION_TASKS) Serial.printf(" %d", kRanks[i]);
    Serial.println();

    Serial.printf("[BENCH] Repeticoes por R: %d\n", BENCH_REPS);
    Serial.printf("[BENCH] Custo do timer descontado por janela: %.3f us\n", gTimerOvhUs);
    Serial.println("[BENCH] Tempos em ms. Ttotal inclui Tgen. Speedup = T1/TR (Ttotal).");
    Serial.println("[BENCH] Perda de pacotes = numero de retransmissoes (tentativas - mensagens).");
    Serial.println("[CSV_RUN_HEADER] modo,nodes,N,ranks,rep,Ttotal_ms,Tgen_ms,Tcomm_ms,"
                   "Tcalc0_ms,TcalcMax_ms,TcommMax_ms,Tred_ms,soma,ok");
    Serial.println(BENCH_CSV_RES_HEADER);
    Serial.println();
}

static void Bench_PrintRowCsv(const ResultRow &r)
{
    Serial.printf("[CSV_RES],%s,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
                  "%.4f,%.4f,%.4f,%lu,%lu,%lu,%lu,%d\n",
                  COMM_NAME, NUM_NODES, BENCH_VECTOR_SIZE, r.R, r.reps,
                  r.total, r.gen, r.comm, r.calc0, r.calcMax, r.commMax, r.red,
                  r.speedup, r.eff, r.speedupCalc,
                  (unsigned long)r.msgs, (unsigned long)r.tx,
                  (unsigned long)r.retx, (unsigned long)r.failed, r.ok ? 1 : 0);
}

static void Bench_PrintRowPretty(const ResultRow &r)
{
    Serial.println("----------------------------------------------------------");
    Serial.printf("[RESULT] N=%d  ranks=%d  nodes=%d  %s  (media de %d exec.)  %s\n",
                  BENCH_VECTOR_SIZE, r.R, NUM_NODES, COMM_NAME, r.reps,
                  r.ok ? "OK" : "*** SOMA INCORRETA ***");
    Serial.printf("[RESULT]   Ttotal=%.3f ms | Tgen=%.3f | Tcomm=%.3f | Tcalc=%.3f (max %.3f) | "
                  "Tred=%.3f | TcommMax=%.3f\n",
                  r.total, r.gen, r.comm, r.calc0, r.calcMax, r.red, r.commMax);
    Serial.printf("[RESULT]   Speedup=%.3f | Eficiencia=%.3f | SpeedupCalc=%.3f\n",
                  r.speedup, r.eff, r.speedupCalc);
    Serial.printf("[RESULT]   (node 0, todas as exec.) mensagens=%lu tentativas=%lu "
                  "retransmissoes=%lu msgPerdidaDeVez=%lu\n",
                  (unsigned long)r.msgs, (unsigned long)r.tx,
                  (unsigned long)r.retx, (unsigned long)r.failed);
    Bench_PrintRowCsv(r);
}

// ---------------- ApplicationTask ---------------------------

void ApplicationTask(void *parameter)
{
    TaskContext *context = (TaskContext *)parameter;

    const int  rank      = context->rank;
    const int  N         = BENCH_VECTOR_SIZE;
    const bool master    = (rank == 0);
    const bool statsRank = (rank == THIS_NODE_ID);   // 1 por node: imprime retransmissões

    if (master) Bench_PrintHeader();

    if (statsRank) Stats_Reset();

    RetxAccum acc;
    LinkStats base[NUM_NODES];
    int32_t   runId = 0;

    MPI_Barrier();      // todos os ranks iniciados

    for (int ri = 0; ri < BENCH_N_RANKS; ri++)
    {
        const int R = kRanks[ri];
        if (R > TOTAL_APPLICATION_TASKS) continue;

        memset(&acc, 0, sizeof(acc));

        // acumuladores de tempo (só o rank 0 usa)
        double sTotal = 0, sGen = 0, sComm = 0, sCalc0 = 0,
               sCalcMax = 0, sCommMax = 0, sRed = 0;
        bool   allOk = true;

        if (master)
            Serial.printf("\n[BENCH] >>> N=%d  ranks=%d  (%d execucoes)\n", N, R, BENCH_REPS);

        for (int rep = 0; rep < BENCH_REPS; rep++)
        {
            runId++;

            if (statsRank) Bench_TakeBase(base);

            MPI_Barrier();                      // sincroniza o início

            BenchRaw raw;
            memset(&raw, 0, sizeof(raw));

            Bench_Run(rank, N, R, runId, raw);  // inclui MPI_Bcast final

            MPI_Barrier();                      // todos terminaram

            if (master)
            {
                double calcMax = raw.rankCalcUs[0];
                double commMax = raw.rankCommUs[0];

                for (int r = 1; r < R; r++)
                {
                    if (raw.rankCalcUs[r] > calcMax) calcMax = raw.rankCalcUs[r];
                    if (raw.rankCommUs[r] > commMax) commMax = raw.rankCommUs[r];
                }

                double total = raw.totalUs / 1000.0;
                double gen   = raw.genUs   / 1000.0;
                double comm  = raw.commUs  / 1000.0;
                double calc0 = raw.calcUs  / 1000.0;
                double red   = raw.redUs   / 1000.0;
                calcMax /= 1000.0;
                commMax /= 1000.0;

                sTotal += total;   sGen += gen;       sComm += comm;
                sCalc0 += calc0;   sCalcMax += calcMax; sCommMax += commMax;
                sRed += red;
                allOk = allOk && raw.ok;

                        Serial.printf("[RUN] N=%d ranks=%d nodes=%d %s rep=%d/%d | "
                              "Ttotal=%.3f Tgen=%.3f Tcomm=%.3f Tcalc0=%.3f "
                              "TcalcMax=%.3f TcommMax=%.3f Tred=%.3f ms | "
                              "maximo=%lld ref=%lld %s\n",
                              N, R, NUM_NODES, COMM_NAME, rep + 1, BENCH_REPS,
                              total, gen, comm, calc0, calcMax, commMax, red,
                              (long long)raw.global, (long long)raw.ref,
                              raw.ok ? "OK" : "*** ERRO ***");

                Serial.printf("[CSV_RUN],%s,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%lld,%d\n",
                              COMM_NAME, NUM_NODES, N, R, rep + 1,
                              total, gen, comm, calc0, calcMax, commMax, red,
                              (long long)raw.global, raw.ok ? 1 : 0);

#if BENCH_PRINT_RANKS
                for (int r = 0; r < R; r++)
                    Serial.printf("[RANK] rank %2d (node %d): Tcalc=%.3f ms  Tcomm=%.3f ms\n",
                                  r, RankToNode(r),
                                  raw.rankCalcUs[r] / 1000.0, raw.rankCommUs[r] / 1000.0);
#endif
            }

            if (statsRank)
            {
                vTaskDelay(Ms(BENCH_SETTLE_MS));    // ACKs finais
                Bench_RetxReport(N, R, rep, base, acc);
            }
        }

        // ---- resultado médio da configuração (R) ----
        if (master)
        {
            ResultRow row;
            memset(&row, 0, sizeof(row));

            row.R = R;  row.reps = BENCH_REPS;
            row.total   = sTotal   / BENCH_REPS;
            row.gen     = sGen     / BENCH_REPS;
            row.comm    = sComm    / BENCH_REPS;
            row.calc0   = sCalc0   / BENCH_REPS;
            row.calcMax = sCalcMax / BENCH_REPS;
            row.commMax = sCommMax / BENCH_REPS;
            row.red     = sRed     / BENCH_REPS;
            row.ok      = allOk;

            if (R == 1)
            {
                gT1Total = row.total;
                gT1Calc  = row.calc0;
            }

            row.speedup     = (gT1Total > 0 && row.total > 0)   ? gT1Total / row.total   : -1.0;
            row.eff         = (row.speedup >= 0) ? row.speedup / R : -1.0;
            row.speedupCalc = (gT1Calc > 0 && row.calcMax > 0)  ? gT1Calc / row.calcMax : -1.0;

            row.msgs   = acc.msgs;
            row.tx     = acc.tx;
            row.retx   = acc.retx;
            row.failed = acc.failed;

            if (gRowCount < (int)(sizeof(gRows) / sizeof(gRows[0])))
                gRows[gRowCount++] = row;

            Bench_PrintRowPretty(row);
        }
    }

    MPI_Barrier();
    vTaskDelay(Ms(1000));

    if (master)
    {
        Serial.println();
        Serial.println("==================== RESUMO FINAL (CSV) ====================");
        Serial.println(BENCH_CSV_RES_HEADER);
        for (int i = 0; i < gRowCount; i++) Bench_PrintRowCsv(gRows[i]);
        Serial.println("============================================================");
    }

    Serial.printf("[BENCH] Rank %d: fim do benchmark\n", rank);
    MPI_Finalize();
}


// ============================================================
// SETUP
// ============================================================

void setup()
{
    Serial.begin(115200);
    delay(1000);

    Serial.println();
    Serial.println("==========================================");
    Serial.printf(" ESP32 DISTRIBUTED MPI KERNEL - NODE %d\n", THIS_NODE_ID);
    Serial.println("==========================================");

    Bench_CalibrateTimer();

    PrintLocalRanks();

    for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
    {
        taskContexts[i].rank         = i;
        taskContexts[i].taskHandle   = nullptr;
        taskContexts[i].receiveQueue = nullptr;
        taskContexts[i].barrierSem   = nullptr;
        taskContexts[i].barrierGen   = 0;
        taskContexts[i].pending      = nullptr;
        taskContexts[i].pendingCount = 0;
        taskContexts[i].finalized    = false;

        barrierArrivedGeneration[i] = -1;
        barrierWaitGen[i]           = -1;
    }

    coordGeneration = 0;
    nextSequence    = 0;

    for (int i = 0; i < NUM_NODES; i++)
    {
        nodeDiscovered[i]   = false;
        nodeIP[i]           = IPAddress(0, 0, 0, 0);
        memset(nodeMAC[i], 0, 6);
        lastDeliveredSeq[i] = -1;
    }

    Stats_Reset();

    dataTxMutex      = xSemaphoreCreateMutex();
    controlTxMutex   = xSemaphoreCreateMutex();
    discoveryTxMutex = xSemaphoreCreateMutex();
    barrierMutex     = xSemaphoreCreateMutex();

    dataAckSemaphore   = xSemaphoreCreateBinary();
    createAckSemaphore = xSemaphoreCreateBinary();
    startAckSemaphore  = xSemaphoreCreateBinary();

    senderQueue = xQueueCreate(SENDER_QUEUE_LENGTH, sizeof(Message));

    if (!dataTxMutex || !controlTxMutex || !discoveryTxMutex || !barrierMutex ||
        !dataAckSemaphore || !createAckSemaphore || !startAckSemaphore ||
        !senderQueue)
    {
        FatalRestart("falha criando filas/semaforos");
    }

#if SINGLE_NODE_MODE
    Serial.println("[COMM] Modo: STANDALONE (NUM_NODES=1) - sem radio, tudo local");
    nodeDiscovered[THIS_NODE_ID] = true;
#else
    Transport_Init();
#endif

    bool ok = true;

    ok &= CreateKernelTask(TaskSender,    "KernelSender",   KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);

#if !SINGLE_NODE_MODE
    ok &= CreateKernelTask(TaskReceiver,  "KernelReceiver", KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);
    ok &= CreateKernelTask(TaskDiscovery, "Discovery",      KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);
    ok &= CreateKernelTask(TaskControl,   "Control",        KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);
#endif

    if (THIS_NODE_ID == 0)
        ok &= CreateKernelTask(TaskMaster, "Master", KERNEL_STACK_SIZE, MASTER_TASK_PRIORITY);

    if (!ok) FatalRestart("falha criando tasks do kernel");

    Serial.println();
    Serial.println("[KERNEL] Kernel inicializado");
    Serial.printf("[KERNEL] Node ID: %d | MPI Comm size: %d\n",
                  THIS_NODE_ID, MPI_Comm_size());
    Serial.println("[KERNEL] Aguardando discovery...");
}


// ============================================================
// LOOP: supervisão do Wi-Fi, stats periódicos e monitor de stack
// ============================================================

void loop()
{
    static uint32_t lastCheck = 0;
    static uint32_t lastStats = 0;

#if COMM_MODE == COMM_WIFI && !SINGLE_NODE_MODE
    if (WiFi.status() != WL_CONNECTED)
    {
        Serial.println("[WIFI] Desconectado - reconectando...");
        WiFi.reconnect();
        vTaskDelay(Ms(2000));
        return;
    }

    if (nodeIP[THIS_NODE_ID] != WiFi.localIP())
        nodeIP[THIS_NODE_ID] = WiFi.localIP();
#endif

#if STATS_PRINT_PERIOD_MS > 0 && !SINGLE_NODE_MODE
    if (millis() - lastStats >= STATS_PRINT_PERIOD_MS)
    {
        lastStats = millis();
        Stats_PrintLocal();
    }
#else
    (void)lastStats;
#endif

#if DEBUG_STACK
    if (millis() - lastCheck > 10000)
    {
        lastCheck = millis();

        for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
        {
            if (taskContexts[i].taskHandle != nullptr)
                Serial.printf("[STACK] APP_%d livre (min): %u bytes\n", i,
                    (unsigned)uxTaskGetStackHighWaterMark(taskContexts[i].taskHandle));
        }
    }
#else
    (void)lastCheck;
#endif

    vTaskDelay(Ms(100));
}