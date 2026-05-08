#include <Arduino.h>
#include "FS.h"
#include "LittleFS.h"

void setup() {
  Serial.begin(115200);
  delay(1000);

  // 🔹 Inicializa o LittleFS
  if (!LittleFS.begin(true)) {
    Serial.println("Erro ao montar LittleFS!");
    return;
  }
  Serial.println("LittleFS montado com sucesso!");

  // 🔹 Criar e escrever em um arquivo
  File file = LittleFS.open("/teste.txt", "w");
  if (!file) {
    Serial.println("Erro ao abrir arquivo para escrita");
    return;
  }

  file.println("Ola, isso é um teste com LittleFS!");
  file.println("ESP32 salvando dados :)");
  file.close();

  Serial.println("Arquivo escrito com sucesso!");

  // 🔹 Ler o arquivo
  file = LittleFS.open("/teste.txt", "r");
  if (!file) {
    Serial.println("Erro ao abrir arquivo para leitura");
    return;
  }

  Serial.println("\nConteudo do arquivo:");
  while (file.available()) {
    Serial.write(file.read());
  }
  file.close();
}

void loop() {
}