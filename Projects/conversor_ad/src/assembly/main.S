#include <avr/io.h>

.global main
.global ADC_vect

main:
    ; 1. Configurar Pino 13 (PB7 / OC0A) como saída
    sbi _SFR_IO_ADDR(DDRB), 7

    ; 2. Configurar Timer0 para Fast PWM
    ; WGM01=1, WGM00=1 (Fast PWM), COM0A1=1 (Non-inverting)
    ldi r16, (1<<COM0A1) | (1<<WGM01) | (1<<WGM00)
    out _SFR_IO_ADDR(TCCR0A), r16

    ; Prescaler = 64 (CS01=1, CS00=1)
    ldi r16, (1<<CS01) | (1<<CS00)
    out _SFR_IO_ADDR(TCCR0B), r16

    ; 3. Configurar ADC
    ; Vref = AVcc (REFS0=1), ADLAR = 1 (Ajuste à esquerda)
    ; MUX = 00000 (Canal A0)
    ldi r16, (1<<REFS0) | (1<<ADLAR)
    sts ADMUX, r16

    ; Habilitar: ADC (ADEN), Auto Trigger (ADATE), Interrupção (ADIE)
    ; Prescaler = 128 (ADPS2:0 = 111)
    ldi r16, (1<<ADEN) | (1<<ADATE) | (1<<ADIE) | (1<<ADPS2) | (1<<ADPS1) | (1<<ADPS0)
    sts ADCSRA, r16

    ; 4. Iniciar a primeira conversão (As próximas ocorrerão sozinhas via Auto Trigger)
    lds r16, ADCSRA
    ori r16, (1<<ADSC)
    sts ADCSRA, r16

    ; 5. Habilitar interrupções globais
    sei

loop:
    ; O microcontrolador está 100% livre!
    ; O loop principal não faz nada em relação ao ADC ou ao PWM.
    rjmp loop


; =========================================
; Rotina de Serviço de Interrupção do ADC
; =========================================
ADC_vect:
    ; Salvar contexto: Proteger R16 e SREG (Registrador de Status)
    push r16
    in r16, _SFR_IO_ADDR(SREG)
    push r16

    ; Ler o valor de 8 bits do ADC (Como ADLAR=1, basta o ADCH)
    lds r16, ADCH

    ; Atualizar o brilho do LED jogando o valor direto no Timer
    out _SFR_IO_ADDR(OCR0A), r16

    ; Restaurar contexto
    pop r16
    out _SFR_IO_ADDR(SREG), r16
    pop r16
    
    ; Retornar da interrupção
    reti