#include <avr/io.h>

.global verificar_corte_motor
.global houve_mudanca_porcentagem

; Reserva 1 byte na memória RAM para guardar a porcentagem anterior
.local porcentagem_anterior
.comm  porcentagem_anterior, 1

; =====================================================================
; FUNÇÃO 1: VERIFICAR CORTE DO MOTOR
; =====================================================================
verificar_corte_motor:
    ; r24 = Low Byte, r25 = High Byte, r1 = Zero Absoluto

    cp   r25, r1        ; Testa se o valor passou de 255 (High Byte > 0)
    brne 2f             ; Se passou, o motor VAI GIRAR (pula pro rótulo 2)
    
    cpi  r24, 5         ; O valor está entre 0 e 255. Compara direto com 5!
    brlo 1f             ; Se for 0, 1, 2, 3 ou 4, o motor VAI PARAR (pula pro rótulo 1)
    
2:                      ; ---- RÓTULO 2: SINAL DE GIRO (FALSE) ----
    ldi  r24, 0         ; O valor é >= 5. Retorna 0 pro C++
    ret                 

1:                      ; ---- RÓTULO 1: SINAL DE CORTE (TRUE) ----
    ldi  r24, 1         ; O valor caiu na zona morta (< 5). Retorna 1 pro C++
    ret

; =====================================================================
; FUNÇÃO 2: DETECTAR MUDANÇA DE PORCENTAGEM (8 bits)
; Retorna true (1) se mudou, ou false (0) se for igual ao ciclo passado
; =====================================================================
houve_mudanca_porcentagem:
    ; A variável 'porcentagem' (16 bits) entra em R24 (baixo) e R25 (alto)
    ; Como a porcentagem só vai até 100, usamos apenas o byte baixo (R24)

    ; 1. Carrega o valor que ficou salvo no ciclo passado da RAM para o registrador R22
    lds  r22, porcentagem_anterior

    ; 2. Compara o valor atual (R24) com o valor antigo (R22)
    cp   r24, r22
    breq valores_iguais        ; Se forem iguais, pula para o fim (retorna 0)

    ; 3. Se chegou aqui, os valores são diferentes! 
    ; Salva a nova porcentagem na RAM para o próximo ciclo
    sts  porcentagem_anterior, r24
    
    ldi  r24, 1                ; Retorna 1 (true -> precisa atualizar o LCD)
    ret

valores_iguais:
    ldi  r24, 0                ; Retorna 0 (false -> não mexe no LCD)
    ret