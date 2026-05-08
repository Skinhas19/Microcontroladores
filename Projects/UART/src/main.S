; ==============================================================================
; SISTEMA DE COMUNICAÇÃO UART - ATmega2560
; Selecione o ambiente (env:transmissor ou env:receptor) no PlatformIO
; ==============================================================================

#define __SFR_OFFSET 0      
#include <avr/io.h>         

#define UBRR_VAL 103       
 ; Baud Rate: 9600 a 16MHz

.global main                

; ------------------------------------------------------------------------------
; MODO TRANSMISSOR
; ------------------------------------------------------------------------------
#ifdef IS_TRANSMISSOR

main:
        ; Inicialização do Stack Pointer
        ldi r16, hi8(RAMEND)
        out SPH, r16
        ldi r16, lo8(RAMEND)
        out SPL, r16

        ; Configura Pinos 53 (PB0) e 52 (PB1) como entrada com Pull-up
        ldi r16, 0x00
        out DDRB, r16       
        ldi r16, 0x03       
        out PORTB, r16      

        ; Configuração da USART0 (TX)
        ldi r16, hi8(UBRR_VAL)
        sts UBRR0H, r16     
        ldi r16, lo8(UBRR_VAL)
        sts UBRR0L, r16

        ldi r16, (1<<TXEN0) ; Habilita TX
        sts UCSR0B, r16     
        ldi r16, (1<<UCSZ01) | (1<<UCSZ00)
        sts UCSR0C, r16     

LOOP_TX:
        ; Leitura e Máscara dos botões
        in r16, PINB        
        andi r16, 0x03      

AGUARDA_TX:
        lds r17, UCSR0A     
        sbrs r17, UDRE0     ; Aguarda buffer liberar
        rjmp AGUARDA_TX     

        sts UDR0, r16       ; Envia pacote via TX0

        rjmp LOOP_TX

#endif ; Fim do código do transmissor


; ------------------------------------------------------------------------------
; MODO RECEPTOR
; ------------------------------------------------------------------------------
#ifdef IS_RECEPTOR

main:
        ; Inicialização do Stack Pointer
        ldi r16, hi8(RAMEND)
        out SPH, r16
        ldi r16, lo8(RAMEND)
        out SPL, r16

        ; Configura Pinos 22 (PA0) e 23 (PA1) como saídas
        ldi r16, 0x03       
        out DDRA, r16       
        ldi r16, 0x00       
        out PORTA, r16      ; LEDs iniciam apagados

        ; Configuração da USART0 (RX)
        ldi r16, hi8(UBRR_VAL)
        sts UBRR0H, r16     
        ldi r16, lo8(UBRR_VAL)
        sts UBRR0L, r16

        ldi r16, (1<<RXEN0) ; Habilita RX
        sts UCSR0B, r16     
        ldi r16, (1<<UCSZ01) | (1<<UCSZ00)
        sts UCSR0C, r16     

LOOP_RX:
AGUARDA_RX:
        lds r17, UCSR0A     
        sbrs r17, RXC0      ; Aguarda pacote chegar
        rjmp AGUARDA_RX     

        lds r16, UDR0       ; Lê o pacote

        ; Inverte e aplica máscara
        com r16             ; 0 vira 1, e 1 vira 0 (Aperto de botão acende LED)
        andi r16, 0x03      

        ; Joga nos LEDs
        out PORTA, r16      

        rjmp LOOP_RX

#endif ; Fim do código do receptor