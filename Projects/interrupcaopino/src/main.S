#define __SFR_OFFSET 0
#include <avr/io.h>
;pino 13 e 12 led
;pino A8 e A9 BOTOES
.global main
.global PCINT2_vect

main:
    ; 1. CONFIGURA OS LEDS (Pinos 12 e 13 / PB6 e PB7)
    sbi DDRB, 6             ; Configura PB6 como saída
    sbi DDRB, 7             ; Configura PB7 como saída

    ; 2. CONFIGURA O PORTK PARA OS BOTÕES (Pinos A8 e A9)
    ldi r16, 0x00
    sts DDRK, r16           ; PORTK como entrada
    ldi r16, 0xFF
    sts PORTK, r16          ; Pull-ups ativados

    ; 3. CONFIGURA A INTERRUPÇÃO PCINT2
    lds r16, PCICR
    ori r16, (1 << PCIE2)   
    sts PCICR, r16

    lds r16, PCMSK2
    ; Habilita A8 (PCINT16) e A9 (PCINT17)
    ori r16, (1 << PCINT16) | (1 << PCINT17) 
    sts PCMSK2, r16

    sei                     

; ------------------------------------------------------------------------------
; LOOP PRINCIPAL
; ------------------------------------------------------------------------------
loop:
    rjmp loop               

; ==============================================================================
; PCINT2_vect: INTERRUPÇÃO PARA O PORTK
; ==============================================================================
PCINT2_vect:
    push r16
    in r16, SREG
    push r16
    push r21
    push r22
    push r23

    ; 4. TESTE DE PINOS E ACIONAMENTO INDIVIDUAL
    lds r16, PINK
    
    ; Teste Botão 1 (A8 / PK0) -> LED 1 (Pino 13 / PB7)
    sbrc r16, 0             
    rjmp check_pin_2     
    sbi PINB, 7             

check_pin_2:
    ; Teste Botão 2 (A9 / PK1) -> LED 2 (Pino 12 / PB6)
    sbrc r16, 1             
    rjmp end_isr            
    sbi PINB, 6             

end_isr:
    ; 5. DELAY DE 200ms (Debounce)
    ldi  r21, 21
    ldi  r22, 75
    ldi  r23, 191
delay_loop:
    dec  r23
    brne delay_loop
    dec  r22
    brne delay_loop
    dec  r21
    brne delay_loop

    ; Limpa flag PCIF2
    ldi r16, (1 << PCIF2)
    out PCIFR, r16

    pop r23
    pop r22
    pop r21
    pop r16
    out SREG, r16
    pop r16
    reti