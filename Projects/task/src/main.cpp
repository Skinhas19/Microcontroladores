#include <Arduino.h>

hw_timer_t *timer0 = NULL;
hw_timer_t *timer1 = NULL;

volatile bool flag0 = false;
volatile bool flag1 = false;

// ================= ISRs =================
void IRAM_ATTR isrCore0() {
  flag0 = true;
}

void IRAM_ATTR isrCore1() {
  flag1 = true;
}

// ================= Setup dos timers =================

// Core 0
void setupTimerCore0(void *pvParameters) {
  timer0 = timerBegin(0, 80, true); // 80MHz / 80 = 1MHz
  timerAttachInterrupt(timer0, &isrCore0, true);
  timerAlarmWrite(timer0, 1000000, true); // 1 segundo
  timerAlarmEnable(timer0);

  vTaskDelete(NULL);
}

// Core 1
void setupTimerCore1(void *pvParameters) {
  timer1 = timerBegin(1, 80, true);
  timerAttachInterrupt(timer1, &isrCore1, true);
  timerAlarmWrite(timer1, 3000000, true); // 3 segundos
  timerAlarmEnable(timer1);

  vTaskDelete(NULL);
}

// ================= Setup =================
void setup() {
  Serial.begin(115200);
  delay(1000);

  xTaskCreatePinnedToCore(setupTimerCore0, "Timer0", 2048, NULL, 1, NULL, 0);
  xTaskCreatePinnedToCore(setupTimerCore1, "Timer1", 2048, NULL, 1, NULL, 1);
}

// ================= Loop =================
void loop() {
  if (flag0) {
    flag0 = false;
    Serial.print("ISR Core 0 executando no núcleo: ");
    Serial.println(xPortGetCoreID());
  }

  if (flag1) {
    flag1 = false;
    Serial.print("ISR Core 1 executando no núcleo: ");
    Serial.println(xPortGetCoreID());
  }
}