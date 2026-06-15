#include <avr/io.h>

.global main        

; --- TABELA DE VETORES DE INTERRUPÇÃO (Endereços exatos do ATmega2560) ---
.section .vectors, "ax"

.org 0x0000
    jmp main            ; Vetor 1: Reset do Sistema

.org 0x002E             ; Endereço exato do TIMER0_OVF no ATmega2560 (Vetor 24)
    jmp TIMER0_OVF


; --- SEÇÃO DE CÓDIGO ---
.section .text      

main:
    ; 1. Inicializa o Stack Pointer (Ponteiro de Pilha)
    ldi r16, hi8(RAMEND)
    out _SFR_IO_ADDR(SPH), r16
    ldi r16, lo8(RAMEND)
    out _SFR_IO_ADDR(SPL), r16

    ; 2. Configura PB5 como Saída
    sbi _SFR_IO_ADDR(DDRB), DDB5

    ; 3. Inicializa o LED desligado
    cbi _SFR_IO_ADDR(PORTB), PORTB5

    ; 4. Configura o Timer0 (Modo Normal)
    ldi r16, 0x00
    out _SFR_IO_ADDR(TCCR0A), r16

    ; Prescaler 1024 (CS02=1 e CS00=1)
    ldi r16, 0x05   
    out _SFR_IO_ADDR(TCCR0B), r16

    ; Habilita interrupção por estouro (TOIE0)
    ldi r16, (1 << TOIE0)
    sts TIMSK0, r16

    ; 5. Habilita Interrupções Globais
    sei

LOOP:
    rjmp LOOP

; --- ROTINA DE INTERRUPÇÃO ---
TIMER0_OVF:
    ; Salva o registrador de status (SREG)
    in r16, _SFR_IO_ADDR(SREG)
    push r16

    ; Inverte o estado do pino PB5 usando o PINB
    ldi r17, (1 << PINB5)
    out _SFR_IO_ADDR(PINB), r17

    ; Restaura o SREG e retorna
    pop r16
    out _SFR_IO_ADDR(SREG), r16
    reti