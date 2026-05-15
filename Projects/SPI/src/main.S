#ifdef MASTER
; ==============================================================================
; ATmega2560 - MESTRE (MASTER) SPI
; ==============================================================================
.equ SPCR, 0x2C
.equ SPSR, 0x2D
.equ SPDR, 0x2E
.equ DDRB, 0x04
.equ PORTB, 0x05
.equ DDRC, 0x07
.equ PORTC, 0x08
.equ PINC, 0x06

.equ SPE, 6
.equ MSTR, 4
.equ SPR0, 0
.equ SPIF, 7

.global main
main:
    ldi r16, (1<<2) | (1<<1) | (1<<0)
    out DDRB, r16
    ldi r16, (1<<0)
    out PORTB, r16
    ldi r16, (1<<SPE) | (1<<MSTR) | (1<<SPR0)
    out SPCR, r16
    ldi r16, (1<<0)
    out DDRC, r16
    ldi r16, (1<<1)
    out PORTC, r16

loop_mestre:
    in r17, PINC
    andi r17, (1<<1)    
    in r16, PORTB
    andi r16, ~(1<<0)
    out PORTB, r16
    out SPDR, r17

espera_spi_mestre:
    in r16, SPSR
    sbrs r16, SPIF
    rjmp espera_spi_mestre

    in r16, PORTB
    ori r16, (1<<0)
    out PORTB, r16
    in r18, SPDR

    cpi r18, 0
    breq liga_led_mestre

desliga_led_mestre:
    in r16, PORTC
    andi r16, ~(1<<0)   
    out PORTC, r16
    rjmp delay_mestre

liga_led_mestre:
    in r16, PORTC
    ori r16, (1<<0)     
    out PORTC, r16

delay_mestre:
    ldi r20, 0x10
d1: ldi r21, 0xFF
d2: dec r21
    brne d2
    dec r20
    brne d1
    rjmp loop_mestre

#elif defined(SLAVE)
; ==============================================================================
; ARDUINO NANO (ATmega328P) - ESCRAVO (SLAVE) SPI
; ==============================================================================
.equ SPCR, 0x2C
.equ SPSR, 0x2D
.equ SPDR, 0x2E
.equ DDRB, 0x04
.equ PORTB, 0x05
.equ DDRD, 0x0A
.equ PORTD, 0x0B
.equ PIND, 0x09

.equ SPE, 6
.equ SPIF, 7

.global main
main:
    ldi r16, (1<<4)
    out DDRB, r16
    ldi r16, (1<<SPE)
    out SPCR, r16
    ldi r16, (1<<2)
    out DDRD, r16
    ldi r16, (1<<3)
    out PORTD, r16

loop_escravo:
    in r17, PIND
    andi r17, (1<<3)
    out SPDR, r17

espera_spi_escravo:
    in r16, SPSR
    sbrs r16, SPIF
    rjmp espera_spi_escravo

    in r18, SPDR
    cpi r18, 0
    breq liga_led_escravo

desliga_led_escravo:
    in r16, PORTD
    andi r16, ~(1<<2)   
    out PORTD, r16
    rjmp loop_escravo

liga_led_escravo:
    in r16, PORTD
    ori r16, (1<<2)     
    out PORTD, r16
    rjmp loop_escravo

#endif