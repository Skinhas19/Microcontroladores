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
    cp   r25, r1               
    brne 2f                    
    cp   r24, r1               
    breq 1f                    
    cpi  r24, 5                
    brlo 1f                    
    jmp  2f                    
1:
    ldi  r24, 1                
    ret
2:
    ldi  r24, 0                
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