#include <avr/io.h>

.global main        

; --- TABELA DE VETORES DE INTERRUPÇÃO ---
.section .vectors, "ax"
.org 0x0000
    jmp main            ; Vetor de Reset (Apenas o reset é necessário aqui)


; --- SEÇÃO DE CÓDIGO ---
.section .text      

main:
    ; 1. Inicializa o Stack Pointer
    ldi r16, hi8(RAMEND)
    out _SFR_IO_ADDR(SPH), r16
    ldi r16, lo8(RAMEND)
    out _SFR_IO_ADDR(SPL), r16

    ; 2. Configura os Pinos Físicos como Saída
    ; OC0A está no pino PB7 -> Configura bit 7 do DDRB
    sbi _SFR_IO_ADDR(DDRB), DDB7

    ; OC0B está no pino PG5 -> Configura bit 5 do DDRG
    ; Nota: DDRG está no espaço estendido, usamos 'sts' em vez de 'sbi'
    lds r16, DDRG
    ori r16, (1 << DDG5)
    sts DDRG, r16

    ; 3. Configura o Modo do Timer0 (CTC com Toggle nos pinos OC0A e OC0B)
    ; COM0A0=1, COM0B0=1 (Toggle) e WGM01=1 (Modo CTC) -> Valor 0x52
    ldi r16, 0x52
    out _SFR_IO_ADDR(TCCR0A), r16

    ; 4. Define os limites de comparação (Valores TOP)
    ldi r16, 200        ; Limite máximo (Valor TOP para OC0A)
    out _SFR_IO_ADDR(OCR0A), r16

    ldi r16, 50         ; Valor de deslocamento para OC0B
    out _SFR_IO_ADDR(OCR0B), r16

    ; 5. Configura Prescaler para 1024 e inicia o Timer
    ; CS02=1 e CS00=1 -> Valor 0x05
    ldi r16, 0x05   
    out _SFR_IO_ADDR(TCCR0B), r16

    ; NOTA: Não habilitamos nenhuma interrupção em TIMSK0 e não usamos 'sei'
    ; porque o próprio circuito interno faz o trabalho nos pinos.

LOOP:
    rjmp LOOP           ; O CPU fica livre aqui enquanto o hardware gera as ondas