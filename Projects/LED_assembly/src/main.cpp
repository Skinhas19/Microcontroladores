#include <Arduino.h>

void setup() {
  // Configura o Pino 13 (Porta B, Bit 7) como saída
  asm volatile ("sbi 0x04, 7\n");
}

void loop() {
  // Pino 13 em HIGH
  asm volatile ("sbi 0x05, 7\n"); 
  delay(500);

  // Pino 13 em LOW
  asm volatile ("cbi 0x05, 7\n"); 
  delay(500);
}