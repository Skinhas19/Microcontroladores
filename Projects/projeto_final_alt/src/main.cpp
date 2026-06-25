#include <Arduino.h>                                    
#include <LiquidCrystal.h>                              

extern "C" bool verificar_corte_motor(int valor_potenciometro);   // Declara a função externa (feita no arquivo Assembly) para checar o corte de giro
extern "C" bool houve_mudanca_porcentagem(int porcentagem_atual); // Declara a função externa (feita no arquivo Assembly) para detectar mudança de valor

LiquidCrystal lcd(12, 11, 5, 4, 3, 2);                 // Inicializa o objeto do LCD mapeando fisicamente os pinos (RS, E, D4, D5, D6, D7)

int pinod = 7;                                         // Define o pino digital 7 (que suporta PWM) como o canal de controle do motor
int velocidadeMinima = 120;                            // Define o valor do PWM de arranque para vencer a inércia física do motor DC

int val = 0;                                           // Cria a variável 'val' para guardar a leitura bruta e instantânea do potenciômetro
int velocidadeMapeada = 0;                             // Cria a variável para guardar a potência final que será enviada ao motor (0 a 255)
int porcentagem = 0;                                   // Cria a variável para guardar a porcentagem visual que vai para a tela (0 a 100)

void setup()                                           
{                                                      
  pinMode(pinod, OUTPUT);                              // Configura eletricamente o pino 7 como uma saída de tensão para o driver do motor

  // ---- ACELERAÇÃO HARDWARE DO ADC ----
  ADCSRA |= (1 << ADPS2) | (1 << ADPS1);               // Acessa o registrador ADCSRA e escreve o nível lógico 1 nos bits ADPS2 e ADPS1
  ADCSRA &= ~(1 << ADPS0);                             // Acessa o registrador ADCSRA e força o nível lógico 0 no bit ADPS0 (Prescaler para 64)
  //fadc=16mhz/64=250khz,normal é 125khz
  lcd.begin(16, 2);                                    // Avisa ao controlador do display que ele tem uma matriz geométrica de 16 colunas e 2 linhas
  
  lcd.setCursor(0, 0);                                 // Move o cursor de escrita para a coluna 0 e linha 0 (canto superior esquerdo)
  lcd.print("MOTOR: PARADO");                          // Imprime o texto estático do status inicial na primeira linha
  lcd.setCursor(0, 1);                                 // Move o cursor de escrita para a coluna 0 e linha 1 (canto inferior esquerdo)
  lcd.print("VELOCIDADE: 0%  ");                       // Imprime o texto estático e o valor inicial da velocidade na segunda linha
}                                                      

void loop()                                            
{                                                      
  val = analogRead(A0);                                // O ADC faz a conversão rápida da tensão no pino A0 e salva o número (0 a 1023) na variável 'val'

  if (verificar_corte_motor(val)) {                    // Chama o Assembly para avaliar a leitura. Se a função retornar 1 (verdadeiro, o motor deve parar):
    velocidadeMapeada = 0;                             // Força a variável de PWM para 0, cortando a energia
    porcentagem = 0;                                   // Força a variável de tela para 0%
  }                                                    // Fechamento da condição verdadeira (if)
  else {                                               // Caso a função em Assembly retorne 0 (falso, o motor deve girar):
    velocidadeMapeada = constrain(map(val, 5, 1015, velocidadeMinima, 255), 0, 255); // Calcula a proporção de potência e corta excessos (Overflow)
    porcentagem = constrain(map(val, 5, 1015, 0, 100), 0, 100);                      // Calcula a proporção para a tela e trava estritamente em 100 máximo
  }                                                    

  analogWrite(pinod, velocidadeMapeada);               // Modifica o hardware do Timer para enviar o PWM calculado ao pino 7 (motor)

  // ---- ATUALIZAÇÃO DO LCD CONTROLADA PELO ASSEMBLY ----
  if (houve_mudanca_porcentagem(porcentagem)) {        // Envia o novo valor para o Assembly. Se for diferente do valor salvo na RAM anterior:

    lcd.setCursor(7, 0);                               // Posiciona o cursor na coluna 7 da linha 0 (exatamente após a palavra "MOTOR: ")
    lcd.print(porcentagem == 0 ? "PARADO" : "LIGADO ");// Operador ternário: escreve "PARADO" se porcentagem for 0, senão escreve "LIGADO "

    lcd.setCursor(12, 1);                              // Posiciona o cursor na coluna 12 da linha 1 (exatamente após "VELOCIDADE: ")
    lcd.print(porcentagem);                            // Imprime a variável numérica contendo o novo valor (0 a 100)
    lcd.print("%   ");                                 // Imprime o símbolo '%' e três espaços extras para apagar qualquer dígito fantasma na tela
  }                                                    
}                                                      