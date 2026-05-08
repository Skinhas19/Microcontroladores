.global main

; A função main é o ponto de entrada do nosso programa
main:
    ; Configura o Pino 13 (Bit 7 da Porta B) como saída
    ; 0x04 é o endereço de I/O do registrador DDRB
    sbi 0x04, 7      

loop:
    ; Liga o LED (Pino 13 em HIGH)
    ; 0x05 é o endereço de I/O do registrador PORTB
    sbi 0x05, 7      
    
    ; Chama a nossa subrotina de atraso
    call delay_500ms

    ; Desliga o LED (Pino 13 em LOW)
    cbi 0x05, 7      
    
    ; Chama o atraso novamente
    call delay_500ms

    ; Pula de volta para o começo do loop (infinito)
    rjmp loop        


; --------------------------------------------------------
; Subrotina de delay de ~500ms (para um clock de 16MHz)
; O processador vai ficar preso aqui diminuindo o valor
; de registradores até chegar a zero.
; --------------------------------------------------------
delay_500ms:
    ldi r20, 41          ; Carrega o valor 41 no registrador r20
delay_outer:
    ldi r21, 255         ; Carrega 255 no r21
delay_middle:
    ldi r22, 255         ; Carrega 255 no r22
delay_inner:
    dec r22              ; Diminui 1 do r22 (1 ciclo)
    brne delay_inner     ; Volta se não for zero (2 ciclos)
    
    dec r21              ; Diminui 1 do r21
    brne delay_middle    ; Volta se não for zero
    
    dec r20              ; Diminui 1 do r20
    brne delay_outer     ; Volta se não for zero
    
    ret                  ; Retorna para a função principal