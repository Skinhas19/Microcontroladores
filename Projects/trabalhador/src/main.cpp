#include <WiFi.h>
const char* ssid = "ESP32_CLUSTER";
const char* password = "12345678";

IPAddress local_IP(192, 168, 50, 1);
IPAddress gateway(192, 168, 50, 1);
IPAddress subnet(255, 255, 255, 0);

WiFiServer server(5000);


void TaskWorker(void *pvParameters)
{
    WiFiClient *client = (WiFiClient *)pvParameters;

    client->println("HELLO WORKER");

    unsigned long lastActivity = millis();

	while (client->connected())
	{
	    if (client->available())
	    {
			String msg = client->readStringUntil('\n');

			Serial.print("Recebido: ");
			Serial.println(msg);

			client->println("ACK");

			lastActivity = millis();
	    }

	    if (millis() - lastActivity > 30000)
	    {
			Serial.println("Worker timeout!");
			break;
	    }

	    vTaskDelay(pdMS_TO_TICKS(10));
	}

    client->stop();
    delete client;

    Serial.println("Worker desconectado");

    vTaskDelete(NULL);
}



void TaskTCPServer(void *pvParameters)
{
    for (;;)
    {
        WiFiClient client = server.available();

        if (client)
        {
            Serial.println("Novo worker conectado!");

            WiFiClient *clientPtr = new WiFiClient(client);

            xTaskCreate(
                TaskWorker,"Worker",4096,clientPtr,1,NULL
            );
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}


void setup()
{
	Serial.begin(9600);
	WiFi.softAPConfig(
		local_IP,
		gateway,
		subnet
	);
	WiFi.softAP(
		ssid,
		password
	);
	
	
	Serial.println("=== MASTER ===");
	Serial.print("IP: ");
	Serial.println(WiFi.softAPIP());
	server.begin();
	xTaskCreate(
		TaskTCPServer,
		"TCP Server",
		4096,
		NULL,
		2,
		NULL
	);
}
void loop()
{
	vTaskDelay(pdMS_TO_TICKS(1000));
}