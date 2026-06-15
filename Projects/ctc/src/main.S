#include <avr/io.h>

.global main        

; --- TABELA DE VETORES DE INTERRUPÇÃO ---
.section .vectors, "ax"

.org 0x0000
    jmp main            

.org 0x002A             ; Endereço exato do TIMER0_COMPA (Comparação canal A)
    jmp TIMER0_COMPA


; --- SEÇÃO DE CÓDIGO ---
.section .text      

main:
    ; 1. Inicializa o Stack Pointer
    ldi r16, hi8(RAMEND)
    out _SFR_IO_ADDR(SPH), r16
    ldi r16, lo8(RAMEND)
    out _SFR_IO_ADDR(SPL), r16

    ; 2. Configura PB5 como Saída
    sbi _SFR_IO_ADDR(DDRB), DDB5
    cbi _SFR_IO_ADDR(PORTB), PORTB5

    ; 3. Configura o Timer0 para Modo CTC
    ; WGM01 = 1 ativa o modo CTC no registrador TCCR0A
    ldi r16, (1 << WGM01)
    out _SFR_IO_ADDR(TCCR0A), r16

    ; 4. Define o limite da contagem (OCR0A = 9)
    ldi r16, 9
    out _SFR_IO_ADDR(OCR0A), r16

    ; 5. Configura Prescaler para 1024 (CS02=1 e CS00=1)
    ldi r16, 0x05   
    out _SFR_IO_ADDR(TCCR0B), r16

    ; 6. Habilita interrupção por Comparação A (OCIE0A)
    ldi r16, (1 << OCIE0A)
    sts TIMSK0, r16

    ; 7. Habilita Interrupções Globais
    sei

LOOP:
    rjmp LOOP           


; --- ROTINA DE INTERRUPÇÃO (Executa a cada 0,64 ms) ---
TIMER0_COMPA:
    in r16, _SFR_IO_ADDR(SREG)
    push r16

    ; Inverte o LED
    ldi r17, (1 << PINB5)
    out _SFR_IO_ADDR(PINB), r17

    pop r16
    out _SFR_IO_ADDR(SREG), r16
    reti