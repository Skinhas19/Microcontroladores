#define __SFR_OFFSET 0
#include <avr/io.h>
;pino 20 pisca
;pino 21 troca estado
;pino 13 pro led

.global main
.global INT0_vect
.global INT1_vect

main:
    ; 1. CONFIGURA O LED (Pino 13 / PB7)
    sbi DDRB, 7             ; Configura PB7 como saída

    ; 2. CONFIGURA OS BOTÕES (Pino 21 = INT0, Pino 20 = INT1)
    cbi DDRD, 0             ; Pino 21 (PD0) como entrada
    sbi PORTD, 0            ; Pull-up interno no Pino 21
    
    cbi DDRD, 1             ; Pino 20 (PD1) como entrada
    sbi PORTD, 1            ; Pull-up interno no Pino 20

    ; 3. CONFIGURA O TIPO DE INTERRUPÇÃO (EICRA)
    ; INT0 -> Transição 1 para 0 (Borda de Descida): ISC01 = 1, ISC00 = 0
    ; INT1 -> Nível Lógico 0: ISC11 = 0, ISC10 = 0
    lds r16, EICRA
    ori r16, (1 << ISC01)   ; Liga o ISC01. Os bits da INT1 já começam em 0 por padrão.
    sts EICRA, r16
    
    ; 4. HABILITA AS INTERRUPÇÕES
    sbi EIMSK, INT0         ; Liga a chave do Pino 21
    sbi EIMSK, INT1         ; Liga a chave do Pino 20
    
    sei                     ; Habilita a chave geral

; ------------------------------------------------------------------------------
; LOOP PRINCIPAL
; ------------------------------------------------------------------------------
loop:
    rjmp loop               ; Fica ocioso aguardando apertar os botões

; ==============================================================================
; INT0: INTERRUPÇÃO POR TRANSIÇÃO (Pino 21)
; ==============================================================================
INT0_vect:
    ; Salva os registradores de contexto (Prática obrigatória quando o código cresce)
    push r16
    in r16, SREG
    push r16

    sbi PINB, 7             ; Inverte o LED (Se tiver bouncing, vai bugar)

    ; Restaura o contexto
    pop r16
    out SREG, r16
    pop r16
    reti

; ==============================================================================
; INT1: INTERRUPÇÃO POR NÍVEL (Pino 20)
; ==============================================================================
INT1_vect:
    ; Salva SREG e os registradores que usaremos no Delay
    push r16
    in r16, SREG
    push r16
    push r21
    push r22
    push r23

    sbi PINB, 7             ; Inverte o LED

    ; ---- DELAY DE APROXIMADAMENTE 200ms (Clock 16MHz) ----
    ; Essa é uma rotina de repetição de 3 níveis para gastar tempo do processador
    ldi r21, 21
    ldi r22, 75
    ldi r23, 191
delay_loop:
    dec r23
    brne delay_loop
    dec r22
    brne delay_loop
    dec r21
    brne delay_loop
    ; ------------------------------------------------------

    ; Restaura os registradores (Ordem inversa!)
    pop r23
    pop r22
    pop r21
    pop r16
    out SREG, r16
    pop r16
    reti                    ; Ao sair, se o botão ainda estiver segurado, ele entra aqui de novo!