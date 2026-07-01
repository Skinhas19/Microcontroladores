#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <UniversalTelegramBot.h>

// --- Definições de Hardware ---
const int SENSOR_PIN = 4;
const int LED_PIN = 3;
const int BUZZER_PIN = 5;
const int BOTAO_PIN = 6;

// --- Credenciais ---
const char* ssid = "A15 de Lucas";
const char* password = "12345678";
const char* BOT_TOKEN = "8898846123:AAHKPzjQ3idqTivADpXt1RVBcaG4Nk28xIE";
const char* CHAT_ID = "7967251086";

// --- Parâmetros ---
const int TEMPO_ANALISE_MS = 300;
const int PULSOS_PARA_DISPARO = 300;

// --- Variáveis Globais e Handles ---
volatile int contadorPulsos = 0;

TaskHandle_t TaskMonitor_Handle = NULL;
TaskHandle_t TaskAlarme_Handle = NULL;
TaskHandle_t TaskComunica_Handle = NULL;

// O Semáforo Binário que vai controlar a parada do alarme
SemaphoreHandle_t Semaforo_Desarme = NULL;

WiFiClientSecure client;
UniversalTelegramBot bot(BOT_TOKEN, client);

// =========================================================================
// INTERRUPÇÃO 1: SENSOR (Gatilho da Intrusão)
// =========================================================================
void IRAM_ATTR ISR_Sensor() {
    contadorPulsos++;
    BaseType_t xHigherPriorityTaskWoken = pdFALSE;
    vTaskNotifyGiveFromISR(TaskMonitor_Handle, &xHigherPriorityTaskWoken);
    if (xHigherPriorityTaskWoken) portYIELD_FROM_ISR();
}

// =========================================================================
// INTERRUPÇÃO 2: BOTÃO (Desarme Manual)
// =========================================================================
void IRAM_ATTR ISR_Botao() {
    BaseType_t xHigherPriorityTaskWoken = pdFALSE;
    
    // Entrega a "chave" do semáforo para a TaskAlarme poder avançar e desligar
    xSemaphoreGiveFromISR(Semaforo_Desarme, &xHigherPriorityTaskWoken);
    
    if (xHigherPriorityTaskWoken) portYIELD_FROM_ISR();
}

// =========================================================================
// TAREFA 1: MONITORAMENTO (Filtro Lógico)
// =========================================================================
void TaskMonitoramento(void *pvParameters) {
    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY); 
        
        vTaskDelay(pdMS_TO_TICKS(TEMPO_ANALISE_MS)); 
        
        if (contadorPulsos >= PULSOS_PARA_DISPARO) {
            xTaskNotifyGive(TaskAlarme_Handle); // Dispara o Alarme
        }
        
        contadorPulsos = 0; 
    }
}

// =========================================================================
// TAREFA 2: CONTROLE DO ALARME (Loop Contínuo e Desarme)
// =========================================================================
void TaskAlarme(void *pvParameters) {
    for (;;) {
        // 1. Espera dormindo até o Sensor confirmar uma intrusão
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        
        // 2. O ROUBO ACONTECEU! Liga tudo indefinidamente.
        digitalWrite(LED_PIN, HIGH);
        digitalWrite(BUZZER_PIN, HIGH);
        
        // 3. Avisa a tarefa de comunicação para mandar 1 única mensagem
        xTaskNotifyGive(TaskComunica_Handle);
        
        // --- NOVO: LÓGICA DE DESARME ---
        // Limpa o semáforo caso alguém tenha apertado o botão por engano antes do alarme tocar
        xSemaphoreTake(Semaforo_Desarme, 0); 
        
        // 4. A tarefa TRAVA AQUI com a sirene gritando. 
        // Ela só vai sair desta linha quando a ISR do Botão entregar o Semáforo.
        xSemaphoreTake(Semaforo_Desarme, portMAX_DELAY);
        
        // 5. Botão foi pressionado! Desliga os atuadores.
        digitalWrite(LED_PIN, LOW);
        digitalWrite(BUZZER_PIN, LOW);
        
        // Zera contagens "fantasmas" que o sensor possa ter gerado devido à vibração da própria sirene
        contadorPulsos = 0; 
        ulTaskNotifyTake(pdTRUE, 0); // Limpa notificações residuais
    }
}

// =========================================================================
// TAREFA 3: COMUNICAÇÃO (Envio Único para o Telegram)
// =========================================================================
void TaskComunicacao(void *pvParameters) {
    client.setInsecure(); 

    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

        bool sucesso = bot.sendMessage(CHAT_ID, "🚨 ALERTA: Sirene ativada!", "");

        if (sucesso) {
            Serial.println("[LOG] Mensagem de alerta enviada.");
        } else {
            Serial.println("[ERRO] Falha no envio via Wi-Fi.");
        }
        
        vTaskDelay(pdMS_TO_TICKS(100)); 
    }
}

// =========================================================================
// SETUP
// =========================================================================
void setup() {
    Serial.begin(115200);
    
    // Pinos de Saída
    pinMode(LED_PIN, OUTPUT);
    pinMode(BUZZER_PIN, OUTPUT);
    
    // Pino do Sensor
    pinMode(SENSOR_PIN, INPUT);
    
    // Pino do Botão configurado com resistor interno de Pull-Up
    pinMode(BOTAO_PIN, INPUT_PULLUP);

    WiFi.begin(ssid, password);
    client.setInsecure(); 

    // Inicializa o Semáforo do Botão
    Semaforo_Desarme = xSemaphoreCreateBinary();

    // Criação das Tarefas
    xTaskCreatePinnedToCore(TaskMonitoramento, "Monitor", 2048, NULL, 3, &TaskMonitor_Handle, 1);
    xTaskCreatePinnedToCore(TaskAlarme, "Alarme", 2048, NULL, 4, &TaskAlarme_Handle, 1);
    xTaskCreatePinnedToCore(TaskComunicacao, "Comunica", 8192, NULL, 1, &TaskComunica_Handle, 1);

    // Registra as duas interrupções de hardware
    attachInterrupt(digitalPinToInterrupt(SENSOR_PIN), ISR_Sensor, FALLING);
    attachInterrupt(digitalPinToInterrupt(BOTAO_PIN), ISR_Botao, FALLING);
    
    Serial.println("Sistema RTOS com Desarme Manual Operacional.");
}

void loop() { 
    vTaskDelete(NULL); 
}