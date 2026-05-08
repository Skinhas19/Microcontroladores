.global main

main:
    ; ----------------------------------------------------
    ; 1. CONFIGURAÇÃO DO BOTÃO (PORTA D, BIT 2)
    ; Endereços: PIND = 0x09, DDRD = 0x0A, PORTD = 0x0B
    ; ----------------------------------------------------
    
    ; cbi (Clear Bit in I/O) coloca '0'. Define pino como ENTRADA
    cbi 0x0A, 2      

    ; sbi (Set Bit in I/O) coloca '1'. Ativa o resistor PULL-UP interno
    sbi 0x0B, 2 

    ; ----------------------------------------------------
    ; 2. CONFIGURAÇÃO DO LED (PORTA B, BIT 7 - Pino 13)
    ; Endereços: DDRB = 0x04, PORTB = 0x05
    ; ----------------------------------------------------
    
    ; Define pino do LED como SAÍDA
    sbi 0x04, 7      

loop:
    ; ----------------------------------------------------
    ; 3. LENDO O BOTÃO E TOMANDO DECISÕES
    ; ----------------------------------------------------
    
    ; A instrução sbic significa "Skip if Bit in I/O is Cleared"
    ; (Pule a próxima instrução se o bit estiver em '0').
    ; Como ativamos o pull-up, o botão pressionado envia '0' (LOW).
    
    sbic 0x09, 2     ; Verifica o bit 2 do registrador PIND
    rjmp desliga_led ; Se o botão NÃO estiver pressionado (lê 1), ele executa esta linha e pula para "desliga_led"

liga_led:
    ; Se o botão estiver pressionado (lê 0), a instrução sbic pulou o "rjmp desliga_led" e caiu direto aqui.
    sbi 0x05, 7      ; Liga o LED (HIGH)
    rjmp loop        ; Volta para o início do loop

desliga_led:
    cbi 0x05, 7      ; Desliga o LED (LOW)
    rjmp loop        ; Volta para o início do loop