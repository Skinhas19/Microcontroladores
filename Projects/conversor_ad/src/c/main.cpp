#include <Arduino.h>
#define pin_d 13
#define pin_a A0
int luz=0;
void setup()
{
    pinMode(pin_d,OUTPUT);
}
void loop()
{   
    luz=analogRead(pin_a);
    analogWrite(pin_d, luz / 4);
}
