#include <avr/io.h>

.global verificar_corte_motor

verificar_corte_motor:
    ; val (16 bits) está em R24 (baixo) e R25 (alto)
    ; 1. Se a parte alta (R25) NÃO for zero, o valor com certeza é maior que 255.
    ; Portanto, o motor NÃO deve desligar (pula direto para o fim).
    cp   r25, r1
    brne 2f                    ; Se R25 != 0, pula para o rótulo 2 (retorna false)
    ; 2. Se a parte alta for zero, testamos se a parte baixa é zero absoluto
    cp   r24, r1
    breq 1f                    ; Se for zero absoluto, pula para o corte (rótulo 1)
    ; 3. Compara a parte baixa com o limite estável de corte (5)
    cpi  r24, 5
    brlo 1f                    ; Se for menor que 5 (1, 2, 3 ou 4), pula para o corte
    jmp  2f                    ; Se for maior ou igual a 5, vai para o fim
1:
    ldi  r24, 1                ; Retorna true (motor deve desligar)
    ret
2:
    ldi  r24, 0                ; Retorna false (motor pode rodar)
    ret