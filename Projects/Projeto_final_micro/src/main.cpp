#include <Arduino.h>
#include <LiquidCrystal.h> 

// Declaração da função externa em AVR Assembly (funcoes.S)
extern "C" bool verificar_corte_motor(int valor_potenciometro);

// Inicialização do LCD em modo de 4 bits: LiquidCrystal lcd(RS, E, D4, D5, D6, D7)
LiquidCrystal lcd(12, 11, 5, 4, 3, 2);

// Variáveis de Configuração e Hardware
int pinod = 7;                     // Pino PWM para controle de velocidade do motor (Hardware do Mega)
const long intervaloLCD = 100;     // Taxa de atualização do display (10 Hz) para evitar flickering
int velocidadeMinima = 90;         // Limiar mínimo de PWM para vencer a inércia física do motor

// Variáveis de Estado e Processamento
int val = 0;                       // Armazena a leitura analógica bruta do potenciômetro (0 a 1023)
int velocidadeMapeada = 0;         // Valor de ciclo de trabalho (Duty Cycle) calculado para o PWM (0 a 255)
int porcentagem = 0;               // Valor escalonado para exibição de velocidade no display (0 a 100%)
unsigned long tempoAnterior = 0;   // Armazena a x'marca de tempo do último ciclo de atualização do LCD

void setup()
{
  pinMode(pinod, OUTPUT);
  lcd.begin(16, 2);
  lcd.setCursor(0, 0); 
  lcd.print("SISTEMA LIGADO!");
  delay(1000);
  lcd.clear(); 
}

void loop()
{
  // Leitura do transdutor analógico (Potenciômetro)
  val = analogRead(A0);

  // Processamento de baixo nível para debounce/zona morta via Assembly
  bool motorDeveDesligar = verificar_corte_motor(val);

  // Intervém no fluxo caso o potenciômetro esteja abaixo do limiar de segurança (val < 5)
  if (motorDeveDesligar) {
    velocidadeMapeada = 0;
    porcentagem = 0;
  } 
  else {
    // Escalonamento matemático dos valores de atuação e exibição
    velocidadeMapeada = map(val, 5, 1023, velocidadeMinima, 255);
    porcentagem = map(val, 5, 1023, 0, 100);
  }

  // Atualiza o Duty Cycle do sinal PWM no pino de potência do motor
  analogWrite(pinod, velocidadeMapeada);

  // ---- ATUALIZAÇÃO DO LCD (NÃO-BLOQUEANTE) ----
  unsigned long tempoAtual = millis();
  if (tempoAtual - tempoAnterior >= intervaloLCD) {
    tempoAnterior = tempoAtual; 

    // Linha 1: Status do Motor
    lcd.setCursor(0, 0); 
    if (porcentagem == 0) {
      lcd.print("MOTOR: PARADO   "); 
    } else {
      lcd.print("MOTOR: LIGADO   ");
    }

    // Linha 2: Velocidade
    lcd.setCursor(0, 1); 
    lcd.print("VELOCIDADE: ");
    lcd.print(porcentagem);
    lcd.print("%   "); 
  }
}