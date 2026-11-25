/*
 * ============================================================
 * ESP32 DISTRIBUTED MPI KERNEL  (versão corrigida)
 * ============================================================
 *
 * Distribuição dos ranks:  rank % NUM_NODES == THIS_NODE_ID
 *
 *   Node 0: 0, 2, 4, 6, 8        Node 1: 1, 3, 5, 7, 9
 *
 * Kernel:  MPI_Comm_rank / MPI_Comm_size / MPI_Wtime (segundos)
 *          MPI_Send / MPI_Recv / MPI_Barrier
 *
 * Principais correções em relação à versão anterior:
 *
 *  1. Discovery nunca termina; o IP dos nodes também é aprendido
 *     pelo remoteIP() de qualquer pacote recebido.
 *  2. Todo TX UDP é protegido por mutex (um WiFiUDP por finalidade).
 *  3. Barreira sem "roubo de token": semáforo por rank + geração
 *     por rank; RELEASE só libera quem espera aquela geração.
 *  4. MPI_Recv usa buffer de mensagens pendentes por rank
 *     (sem busy-wait, sem reordenar, sem perder mensagens).
 *  5. Confiabilidade: dados com ACK + retransmissão + deduplicação;
 *     barreira com reenvio; CREATE/START com retentativas.
 *  7. Application Tasks só começam após notificação de START
 *     (sem vTaskSuspend, sem race entre cores).
 *  8. Geração do RELEASE é sempre copiada dentro do mutex.
 *  9. Nenhum xQueueSend com portMAX_DELAY nas tasks do kernel.
 * 10. Delays nunca viram 0 ticks.
 * 11. Stacks maiores (+ monitor opcional de high-water mark).
 *
 * Node 0 é o coordenador da barreira e o master.
 * Para o Node 1, compile com  -DTHIS_NODE_ID=1  ou edite abaixo.
 * ============================================================
 */

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiUdp.h>

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>

#include <esp_timer.h>


// ============================================================
// CONFIGURAÇÃO DO KERNEL
// ============================================================

#ifndef THIS_NODE_ID
#define THIS_NODE_ID                1       // <<< 1 na segunda placa
#endif

#define NUM_NODES                   2
#define TOTAL_APPLICATION_TASKS     2

static_assert(THIS_NODE_ID >= 0 && THIS_NODE_ID < NUM_NODES,
              "THIS_NODE_ID invalido");

#define DATA_PORT                   5000
#define DISCOVERY_PORT              5001
#define CONTROL_PORT                5002

#define MESSAGE_PAYLOAD             500

#define QUEUE_LENGTH                16      // fila de entrada por rank
#define SENDER_QUEUE_LENGTH         32
#define PENDING_MAX                 24      // mensagens "inesperadas" por rank

#define APP_STACK_SIZE              8192
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

#define DEBUG_STACK                 0       // 1 = imprime high-water marks

#define MPI_ANY_SOURCE              (-1)


// ============================================================
// CONFIGURAÇÃO WI-FI  (preencha com os seus dados)
// ============================================================

const char* WIFI_SSID     = "A15 de Lucas";
const char* WIFI_PASSWORD = "12345678";


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
    CONTROL_BARRIER_RELEASE = 6
};

typedef struct __attribute__((packed))
{
    uint8_t type;
    uint8_t srcNode;        // quem enviou (permite aprender o IP)
    int32_t rank;
    int32_t generation;
    int32_t status;
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

    SemaphoreHandle_t barrierSem;     // binário, só deste rank
    int32_t           barrierGen;     // próxima geração deste rank

    Message          *pending;        // mensagens que chegaram fora de ordem
    int               pendingCount;   // acessado só pela própria task
} TaskContext;


// ============================================================
// PROTÓTIPOS
// ============================================================

void ApplicationTask(void *parameter);

void TaskSender(void *parameter);
void TaskReceiver(void *parameter);
void TaskDiscovery(void *parameter);
void TaskControl(void *parameter);
void TaskMaster(void *parameter);


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

// Um mutex por objeto de TX (beginPacket/write/endPacket não é atômico)
SemaphoreHandle_t dataTxMutex;
SemaphoreHandle_t controlTxMutex;
SemaphoreHandle_t discoveryTxMutex;

IPAddress nodeIP[NUM_NODES];
volatile bool nodeDiscovered[NUM_NODES];

QueueHandle_t senderQueue;

// ACK de dados (o TaskSender só tem 1 mensagem em voo por vez)
SemaphoreHandle_t dataAckSemaphore;
volatile uint32_t lastAckSeq = 0;

// Deduplicação no receptor: última sequência entregue por node de origem
int64_t lastDeliveredSeq[NUM_NODES];

// ACKs de controle
SemaphoreHandle_t createAckSemaphore;
SemaphoreHandle_t startAckSemaphore;
volatile int lastCreateAckRank   = -1;
volatile int lastCreateAckStatus = 0;
volatile int lastStartAckStatus  = 0;

volatile bool kernelReady = false;

// ---- Barreira ------------------------------------------------
// Coordenador (node 0):
//   coordGeneration          geração em coleta
//   barrierArrivedGeneration geração em que cada rank chegou (-1 = nenhuma)
// Todos os nodes:
//   barrierWaitGen[rank]     geração em que o rank está bloqueado (-1 = livre)

int32_t coordGeneration = 0;
int32_t barrierArrivedGeneration[TOTAL_APPLICATION_TASKS];
volatile int32_t barrierWaitGen[TOTAL_APPLICATION_TASKS];
SemaphoreHandle_t barrierMutex;

// ---- Sequência ------------------------------------------------
volatile uint32_t nextSequence = 0;
static portMUX_TYPE sequenceMux = portMUX_INITIALIZER_UNLOCKED;


// ============================================================
// AUXILIARES
// ============================================================

// pdMS_TO_TICKS pode dar 0 se o tick for < 1 kHz: nunca aceitamos 0.
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

// Registra/atualiza o IP de outro node.
void LearnNode(int nodeId, IPAddress ip)
{
    if (nodeId < 0 || nodeId >= NUM_NODES || nodeId == THIS_NODE_ID) return;
    if (ip == IPAddress(0, 0, 0, 0)) return;

    if (!nodeDiscovered[nodeId] || nodeIP[nodeId] != ip)
    {
        nodeIP[nodeId] = ip;
        nodeDiscovered[nodeId] = true;

        Serial.printf("[DISCOVERY] Node %d: %s\n",
                      nodeId, ip.toString().c_str());
    }
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

// Segundos, como no padrão MPI.
double MPI_Wtime()
{
    return (double)esp_timer_get_time() / 1000000.0;
}


// ============================================================
// ENVIO UDP (protegido por mutex)
// ============================================================

bool SendControlMessage(IPAddress destination, const ControlMessage &message)
{
    xSemaphoreTake(controlTxMutex, portMAX_DELAY);

    bool ok = udpControlTx.beginPacket(destination, CONTROL_PORT);

    if (ok)
    {
        udpControlTx.write((const uint8_t *)&message, sizeof(ControlMessage));
        ok = (udpControlTx.endPacket() == 1);
    }

    xSemaphoreGive(controlTxMutex);
    return ok;
}

bool SendDataPacket(IPAddress destination, const Message &msg)
{
    xSemaphoreTake(dataTxMutex, portMAX_DELAY);

    bool ok = udpDataTx.beginPacket(destination, DATA_PORT);

    if (ok)
    {
        udpDataTx.write((const uint8_t *)&msg, sizeof(Message));
        ok = (udpDataTx.endPacket() == 1);
    }

    xSemaphoreGive(dataTxMutex);
    return ok;
}

ControlMessage MakeControl(uint8_t type, int32_t rank,
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
// MPI_Send
// ============================================================
// Retorna quando o kernel aceitou a mensagem (semântica "buffered").
// O kernel garante entrega (ACK + retransmissão) em segundo plano.

bool MPI_Send(const void *buffer, int count, int destinationRank)
{
    TaskContext *context = GetCurrentTaskContext();

    if (context == nullptr)
    {
        Serial.println("[MPI] MPI_Send: contexto inexistente");
        return false;
    }

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
// Mantém um buffer de mensagens pendentes por rank. Mensagens de
// outra origem são guardadas na ordem de chegada e consultadas
// antes de bloquear de novo na fila. Bloqueia de verdade
// (sem taskYIELD / busy-wait).

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

    // 1) primeiro procura nas pendentes (mais antiga primeiro)
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

    // 2) bloqueia na fila até chegar uma mensagem adequada
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
//
// Coordenador = node 0. Cada rank tem sua própria geração.
//
//  - Rank chega: barrierWaitGen[rank] = g; avisa o coordenador.
//  - Node != 0 reenvia a chegada a cada BARRIER_RESEND_MS até ser liberado.
//  - Coordenador: chegada duplicada é idempotente; chegada de geração
//    antiga (RELEASE perdido) provoca reenvio do RELEASE.
//  - RELEASE(g) libera SOMENTE ranks locais com barrierWaitGen == g,
//    cada um pelo seu próprio semáforo -> sem roubo de token.

static void SendBarrierRelease(int node, int32_t generation)
{
    if (node < 0 || node >= NUM_NODES || node == THIS_NODE_ID) return;
    if (!nodeDiscovered[node]) return;

    ControlMessage rel = MakeControl(CONTROL_BARRIER_RELEASE, -1, generation, 1);
    SendControlMessage(nodeIP[node], rel);
}

// Libera ranks locais que esperam a geração informada (idempotente).
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

// Só o coordenador executa isto.
static void Barrier_HandleArrival(int rank, int32_t generation)
{
    if (rank < 0 || rank >= TOTAL_APPLICATION_TASKS) return;

    bool    completed = false;
    bool    stale     = false;
    int32_t releaseGen = generation;   // copiado DENTRO do mutex

    xSemaphoreTake(barrierMutex, portMAX_DELAY);

    if (generation < coordGeneration)
    {
        stale = true;                  // RELEASE dessa geração foi perdido
    }
    else if (generation == coordGeneration)
    {
        barrierArrivedGeneration[rank] = generation;

        int count = 0;
        for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
            if (barrierArrivedGeneration[i] == generation) count++;

        Serial.printf("[BARRIER] Rank %d chegou, geracao %ld (%d/%d)\n",
                      rank, (long)generation, count, TOTAL_APPLICATION_TASKS);

        if (count == TOTAL_APPLICATION_TASKS)
        {
            completed = true;
            releaseGen = generation;
            coordGeneration = generation + 1;
        }
    }
    // generation > coordGeneration: impossível; ignora.

    xSemaphoreGive(barrierMutex);

    if (completed)
    {
        Serial.printf("[BARRIER] Geracao %ld completa - liberando\n",
                      (long)releaseGen);

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

    int     rank       = context->rank;
    int32_t generation = context->barrierGen++;   // só esta task mexe nisto

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
        // Reenvia a chegada até ser liberado (UDP pode perder pacotes).
        while (true)
        {
            if (nodeDiscovered[0])
            {
                ControlMessage m = MakeControl(CONTROL_BARRIER, rank, generation, 0);
                SendControlMessage(nodeIP[0], m);
            }

            if (xSemaphoreTake(context->barrierSem, Ms(BARRIER_RESEND_MS)) == pdTRUE)
                break;
        }
    }

    Serial.printf("[BARRIER] Rank %d liberado da geracao %ld\n",
                  rank, (long)generation);
}


// ============================================================
// CRIAÇÃO / START DAS APPLICATION TASKS
// ============================================================

// Ponto de entrada real da task: espera o START ANTES de rodar a
// aplicação. Assim não há race entre xTaskCreate() e a escrita do
// taskHandle, nem entre "criar" e "suspender".
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

    if (context->taskHandle != nullptr) return true;   // idempotente

    Serial.printf("[KERNEL] Criando rank %d...\n", rank);

    context->rank         = rank;
    context->barrierGen   = 0;
    context->pendingCount = 0;

    context->receiveQueue = xQueueCreate(QUEUE_LENGTH, sizeof(Message));
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

        if (xTaskCreate(KernelTaskEntry, taskName, APP_STACK_SIZE,
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
    if (kernelReady) return;      // idempotente (START pode ser reenviado)

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
// TASK SENDER
// ============================================================
// senderQueue -> destino local (fila do rank) ou remoto (UDP com
// ACK, até DATA_MAX_RETRIES tentativas). Uma mensagem em voo por
// vez, o que preserva a ordem.

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

            if (dst == nullptr || dst->receiveQueue == nullptr ||
                xQueueSend(dst->receiveQueue, &msg, Ms(1000)) != pdTRUE)
            {
                Serial.printf("[KERNEL] Falha entrega local %d -> %d\n",
                              (int)msg.srcRank, (int)msg.dstRank);
            }
            continue;
        }

        // ---------------- REMOTO ----------------
        bool acked = false;

        for (int attempt = 0; attempt < DATA_MAX_RETRIES && !acked; attempt++)
        {
            if (!nodeDiscovered[destinationNode])
            {
                vTaskDelay(Ms(100));
                continue;
            }

            // descarta ACKs velhos
            while (xSemaphoreTake(dataAckSemaphore, 0) == pdTRUE) {}

            if (!SendDataPacket(nodeIP[destinationNode], msg))
            {
                vTaskDelay(Ms(10));
                continue;
            }

            TickType_t start   = xTaskGetTickCount();
            TickType_t timeout = Ms(DATA_ACK_TIMEOUT_MS);

            while (!acked)
            {
                TickType_t elapsed = xTaskGetTickCount() - start;
                if (elapsed >= timeout) break;

                if (xSemaphoreTake(dataAckSemaphore, timeout - elapsed) == pdTRUE)
                {
                    if (lastAckSeq == msg.sequence) acked = true;
                }
            }
        }

        if (!acked)
        {
            Serial.printf("[KERNEL] ERRO: sem ACK de %d -> %d (seq %lu)\n",
                          (int)msg.srcRank, (int)msg.dstRank,
                          (unsigned long)msg.sequence);
        }
    }
}


// ============================================================
// TASK RECEIVER
// ============================================================

void TaskReceiver(void *parameter)
{
    while (true)
    {
        int packetSize = udpDataRx.parsePacket();

        if (packetSize <= 0)
        {
            vTaskDelay(Ms(1));
            continue;
        }

        if (packetSize != (int)sizeof(Message))
        {
            while (udpDataRx.available()) udpDataRx.read();
            continue;
        }

        IPAddress remoteIP = udpDataRx.remoteIP();

        Message msg;
        int received = udpDataRx.read((uint8_t *)&msg, sizeof(Message));

        if (received != (int)sizeof(Message)) continue;

        // ---------- ACK de dados ----------
        if (msg.type == MSG_DATA_ACK)
        {
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

        LearnNode(srcNode, remoteIP);

        TaskContext *dst = GetTaskContext(msg.dstRank);

        if (dst == nullptr || dst->receiveQueue == nullptr) continue; // sem ACK: remetente tenta de novo

        // Retransmissão da última mensagem já entregue: só reenvia ACK.
        bool duplicate = (lastDeliveredSeq[srcNode] == (int64_t)msg.sequence);
        bool delivered = duplicate;

        if (!duplicate)
        {
            delivered = (xQueueSend(dst->receiveQueue, &msg, Ms(50)) == pdTRUE);

            if (delivered)
                lastDeliveredSeq[srcNode] = (int64_t)msg.sequence;
            else
                Serial.printf("[KERNEL] Fila do rank %d cheia - sem ACK\n",
                              (int)msg.dstRank);
        }

        if (delivered)
        {
            Message ack;
            memset(&ack, 0, sizeof(ack));
            ack.type     = MSG_DATA_ACK;
            ack.srcRank  = msg.dstRank;
            ack.dstRank  = msg.srcRank;
            ack.sequence = msg.sequence;

            SendDataPacket(remoteIP, ack);
        }
    }
}


// ============================================================
// DISCOVERY (nunca termina)
// ============================================================

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

        // Continua anunciando para sempre (mais devagar depois de completo),
        // assim um node que reinicia ainda nos encontra.
        uint32_t interval = allFound ? 5000 : 1000;
        uint32_t now = millis();

        if (now - lastSend >= interval)
        {
            SendDiscovery();
            lastSend = now;
        }

        int packetSize = udpDiscoveryRx.parsePacket();

        if (packetSize == (int)sizeof(DiscoveryMessage))
        {
            IPAddress remoteIP = udpDiscoveryRx.remoteIP();

            DiscoveryMessage msg;
            int received = udpDiscoveryRx.read((uint8_t *)&msg, sizeof(msg));

            if (received == (int)sizeof(msg) && msg.magic == DISCOVERY_MAGIC)
                LearnNode(msg.nodeId, remoteIP);
        }
        else if (packetSize > 0)
        {
            while (udpDiscoveryRx.available()) udpDiscoveryRx.read();
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

        if (!SendControlMessage(nodeIP[destinationNode], msg))
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
                // ignora ACK de outro rank (atrasado)
                if (lastCreateAckRank == rank)
                {
                    if (lastCreateAckStatus == 1)
                    {
                        Serial.printf("[MASTER] Rank %d confirmado pelo node %d\n",
                                      rank, destinationNode);
                        return true;
                    }
                    return false;   // node respondeu com erro
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

        if (!SendControlMessage(nodeIP[destinationNode], msg))
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
            vTaskDelay(Ms(500));    // o TaskDiscovery cuida do broadcast
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
        int packetSize = udpControlRx.parsePacket();

        if (packetSize <= 0)
        {
            vTaskDelay(Ms(1));
            continue;
        }

        if (packetSize != (int)sizeof(ControlMessage))
        {
            while (udpControlRx.available()) udpControlRx.read();
            continue;
        }

        IPAddress remoteIP = udpControlRx.remoteIP();

        ControlMessage msg;
        int received = udpControlRx.read((uint8_t *)&msg, sizeof(ControlMessage));

        if (received != (int)sizeof(ControlMessage)) continue;

        // Aprende o IP de quem nos escreveu (resolve discovery unilateral)
        LearnNode(msg.srcNode, remoteIP);

        switch (msg.type)
        {
        case CONTROL_CREATE_TASK:
        {
            Serial.printf("[CONTROL] CREATE rank %d recebido\n", (int)msg.rank);

            bool ok = Kernel_CreateApplicationTask(msg.rank);

            ControlMessage ack = MakeControl(CONTROL_CREATE_ACK, msg.rank, 0, ok ? 1 : 0);
            SendControlMessage(remoteIP, ack);
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

            Kernel_StartLocalTasks();       // idempotente

            ControlMessage ack = MakeControl(CONTROL_START_ACK, -1, 0, 1);
            SendControlMessage(remoteIP, ack);
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
                Serial.printf("[BARRIER] RELEASE geracao %ld recebido\n",
                              (long)msg.generation);
                Barrier_ReleaseLocal(msg.generation);   // idempotente
            }
            break;

        default:
            break;
        }
    }
}


// ============================================================
// ============================================================
// A PARTIR DAQUI COMEÇA A APLICAÇÃO
// ============================================================
// ============================================================
//
// Anel:  0 -> 1 -> 2 -> ... -> 9 -> 0
// Cada rank recebe do anterior, incrementa e envia ao próximo.
// O rank 0 inicia o processo.
// ============================================================

void ApplicationTask(void *parameter)
{
    TaskContext *context = (TaskContext *)parameter;

    int rank         = context->rank;
    int nextRank     = (rank + 1) % TOTAL_APPLICATION_TASKS;
    int previousRank = (rank - 1 + TOTAL_APPLICATION_TASKS) % TOTAL_APPLICATION_TASKS;

    Serial.printf("[APP] Rank %d iniciou: anterior=%d, proximo=%d\n",
                  rank, previousRank, nextRank);

    // Sincroniza todos os ranks antes de começar (exercita a barreira)
    MPI_Barrier();

    if (rank == 0)
    {

        while (true)
        {
            double t0 = MPI_Wtime();

            Serial.printf("[APP] Rank 0: iniciando e populando vetor \n");
            int vet[250];
            for (int i = 0; i < 250; i++)
                vet[i] = rand() % 1000;
            Serial.printf("[APP] Rank 0: Vetor populado, enviando para outros \n");


            for (int i = 1; i < NUM_NODES; i++){
                int idx = i * (250 / NUM_NODES);    
                MPI_Send(vet+idx, sizeof(int) * 250/NUM_NODES, i);
                Serial.printf("[APP] Rank 0: IDX %d enviado para rank %d \n", idx, i);
            }

            int maiorvalor = vet[0];
            for (int i = 0; i < 50; i++){
                if (maiorvalor < vet[i])
                    maiorvalor = vet[i];
            }

            //MPI_Send(&valor, sizeof(valor), nextRank);

            int valorRecv = 0;
            for (int i = 1; i < NUM_NODES; i++){
                MPI_Recv(&valorRecv, sizeof(valorRecv), i);
                if (maiorvalor < valorRecv)
                    maiorvalor = valorRecv;
            }


                

            Serial.printf("[APP] Rank 0 recebeu maior valor %d (volta em %.3f s)\n",
                          maiorvalor, MPI_Wtime() - t0);

            vTaskDelay(Ms(1000));
        }
    }
    else
    {
        int vet[50];
        while (true)
        {
            MPI_Recv(&vet, sizeof(vet), previousRank);

            Serial.printf("[APP] Rank %d recebeu vetor\n",rank);

            int maiorvalor = vet[0];
            for (int i = 0; i < 50; i++){
                if (maiorvalor < vet[i])
                    maiorvalor = vet[i];
            }

            Serial.printf("[APP] Rank %d: maior valor local %d, enviando para rank %d\n",
                          rank, maiorvalor, nextRank);
            
            MPI_Send(&maiorvalor, sizeof(maiorvalor), nextRank);
        }
    }
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

    // ---------- estado ----------
    for (int i = 0; i < TOTAL_APPLICATION_TASKS; i++)
    {
        taskContexts[i].rank         = i;
        taskContexts[i].taskHandle   = nullptr;
        taskContexts[i].receiveQueue = nullptr;
        taskContexts[i].barrierSem   = nullptr;
        taskContexts[i].barrierGen   = 0;
        taskContexts[i].pending      = nullptr;
        taskContexts[i].pendingCount = 0;

        barrierArrivedGeneration[i] = -1;
        barrierWaitGen[i]           = -1;
    }

    coordGeneration = 0;
    nextSequence    = 0;

    for (int i = 0; i < NUM_NODES; i++)
    {
        nodeDiscovered[i]   = false;
        nodeIP[i]           = IPAddress(0, 0, 0, 0);
        lastDeliveredSeq[i] = -1;
    }

    // ---------- Wi-Fi ----------
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);              // sem modem-sleep: menos latência/perda
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

    // ---------- semáforos / mutexes / filas ----------
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

    // ---------- UDP ----------
    if (!udpDataRx.begin(DATA_PORT))           FatalRestart("DATA_PORT");
    if (!udpDiscoveryRx.begin(DISCOVERY_PORT)) FatalRestart("DISCOVERY_PORT");
    if (!udpControlRx.begin(CONTROL_PORT))     FatalRestart("CONTROL_PORT");

    // ---------- tasks do kernel ----------
    bool ok = true;

    ok &= CreateKernelTask(TaskSender,    "KernelSender",   KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);
    ok &= CreateKernelTask(TaskReceiver,  "KernelReceiver", KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);
    ok &= CreateKernelTask(TaskDiscovery, "Discovery",      KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);
    ok &= CreateKernelTask(TaskControl,   "Control",        KERNEL_STACK_SIZE, KERNEL_TASK_PRIORITY);

    if (THIS_NODE_ID == 0)
        ok &= CreateKernelTask(TaskMaster, "Master", KERNEL_STACK_SIZE, MASTER_TASK_PRIORITY);

    if (!ok) FatalRestart("falha criando tasks do kernel");

    Serial.println();
    Serial.println("[KERNEL] Kernel inicializado");
    Serial.printf("[KERNEL] Node ID: %d | MPI Comm size: %d\n",
                  THIS_NODE_ID, MPI_Comm_size());
    Serial.println("[KERNEL] MPI_Wtime() = segundos");
    Serial.println("[KERNEL] Aguardando discovery...");
}


// ============================================================
// LOOP: supervisão do Wi-Fi (e monitor opcional de stack)
// ============================================================

void loop()
{
    static uint32_t lastCheck = 0;

    if (WiFi.status() != WL_CONNECTED)
    {
        Serial.println("[WIFI] Desconectado - reconectando...");
        WiFi.reconnect();
        vTaskDelay(Ms(2000));
        return;
    }

    // se o IP mudou depois de reconectar, atualiza o registro local
    if (nodeIP[THIS_NODE_ID] != WiFi.localIP())
        nodeIP[THIS_NODE_ID] = WiFi.localIP();

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