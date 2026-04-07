// "Ad Astra Per Aspera"
// "Por asperos caminhos até as Estrelas"
// "Para as estrelas através das dificuldades"

/*
 
  456
  123
  789
  
 */


#include <Arduino.h>
#ifdef ESP8266
 #include <ESP8266HTTPClient.h>
 #include <ESP8266WiFi.h>
 #include <WiFiManager.h>             //  https://github.com/tzapu/WiFiManager
 #include <ESP8266HTTPUpdateServer.h> //  Biblioteca que cria o servico de atualizacão via wifi (ou Over The Air - OTA)
 #include <ESP8266mDNS.h>
 #include <PubSubClient.h>
#endif
#ifdef ESP32
 #include <WiFi.h>
#endif

#include <NTPClient.h>                //  https://randomnerdtutorials.com/esp8266-nodemcu-date-time-ntp-client-server-arduino/
#include <WiFiUdp.h>

#include <FS.h>

// Constantes
#define BAUD_RATE         115200

#define NTP_SERVER     "pool.ntp.org"
#define UTC_OFFSET     -3 * 3600
#define UTC_OFFSET_DST 0

#define SEGUNDO           (1000L)
#define MINUTO            (60L * SEGUNDO)
#define MEIA_HORA         (30L * MINUTO)
#define HORA              (60L * MINUTO)

// configuração salva
#define ARQ_CNF           "/arquivo.conf"
#define ARQ_CNF_TEMPO     "/tempo.conf"
#define ARQ_SENSOR        "/sensor.conf"
#define ARQ_TEMP_SENSOR   "/sensor.temp"


#define TEXTO_ABERTURA    "Power Grid Sentinel"
#define VERSAO            "1.03"
#define HOST_NOME         "ESP8266-PowerGridSentinel"
#define SSID_NOME         "PowerGridSentinel"
#define PASS_NOME         ""

//Pinos NodeMCU
#define PIN_D0    16
#define PIN_D1     5
#define PIN_D2     4
#define PIN_D3     0
#define PIN_D4     2
#define PIN_D5    14
#define PIN_D6    12
#define PIN_D7    13
#define PIN_D8    15
//defines - ---
#define PIN_D9     3
#define PIN_D10    1

//Pinos ESP-01
#define GPIO0 0           // GPIO0   -- rele tasmota
#define TX    1           // GPIO1  --- Precisa validar
#define GPIO2 2           // GPIO2
#define RX    3           // GPIO3

#define ESP01

#ifdef ESP01
  #define PIN_BOTAO RX // GPIO0
  #define PIN_LED   GPIO2
#else
  #define PIN_BOTAO PIN_D1
  #define PIN_LED   PIN_D2
#endif


// Variaveis publicas
unsigned long lastChange = 0;
unsigned long WatchdogMQTT_Contador = 0;
bool Ativo = true; 
unsigned long TempoDeEspera = 2L * SEGUNDO;
bool bPrimeiraVez = true;
bool shouldSaveConfig = false;  // flag for saving data
int UltimoValor = 0xff;  // valor deve ser diferente de 0 e 1 
int TimeCounter = 0;


struct Device
{
  String Host; 
  String IPMqtt; 
  String Porta;
  String Mqttuser;
  String Mqttpass;
  String MqttNomeAmigavel;
  String MqttTopico;
};
Device dev;


// Objetos
ESP8266HTTPUpdateServer atualizadorOTA; //Este e o objeto que permite atualizacao do programa via wifi (OTA)
ESP8266WebServer server(80);
WiFiManager wfm;


//Variáveis
WiFiClient wifiClient;
PubSubClient ConexaoMQTT(wifiClient);


//callback notifying us of the need to save config
void saveConfigCallback () 
{
  Serial.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Should save config");
  shouldSaveConfig =   true;
}

void setupWiFiAuto()
{
  // Para o AP para a configuração inicial
  const char *ssid = SSID_NOME;
  const char *pwd  = PASS_NOME;
  char Host [32 + 1] = SSID_NOME;
  char IPMqtt [255 + 1] = "127.0.0.1";
  char Porta [6 + 1] = "1883";
  char Mqttuser [64 + 1] = "mqtt";
  char Mqttpass [64 + 1] = "mqtt";
  char MqttNomeAmigavel [64 + 1] = "PowerGridSentinel";
  char MqttTopico [64 + 1] = "PowerGridSentinel";
  char tmp[40];
  char ssidX[32 + 1];

  carregaConfigEx();
  if (dev.Host.length() != 0)      dev.Host.toCharArray(Host, 32);
  if (dev.IPMqtt.length() != 0)    dev.IPMqtt.toCharArray(IPMqtt, 255);
  if (dev.Porta.length() != 0)     dev.Porta.toCharArray(Porta, 6);
  if (dev.Mqttuser.length() != 0)  dev.Mqttuser.toCharArray(Mqttuser, 64);
  if (dev.Mqttpass.length() != 0)  dev.Mqttpass.toCharArray(Mqttpass, 64);
  if (dev.MqttNomeAmigavel.length() != 0)  dev.MqttNomeAmigavel.toCharArray(MqttNomeAmigavel, 64);
  if (dev.MqttTopico.length() != 0)        dev.MqttTopico.toCharArray(MqttTopico, 64);

  WiFiManagerParameter parameterHost("host",   "Host",    Host,  sizeof(Host) - 1);
  WiFiManagerParameter parameterIPMqtt("IPMqtt",  "IPMqtt", IPMqtt, sizeof(IPMqtt) - 1);
  WiFiManagerParameter parameterPorta("Porta", "Porta",   Porta, sizeof(Porta) - 1);
  WiFiManagerParameter parameterMqttuser("Mqttuser", "Mqttuser",   Mqttuser, sizeof(Mqttuser) - 1);
  WiFiManagerParameter parameterMqttpass("Mqttpass", "Mqttpass",   Mqttpass, sizeof(Mqttpass) - 1);
  WiFiManagerParameter parameterMqttNomeAmigavel("MqttNomeAmigavel", "MqttNomeAmigavel",   MqttNomeAmigavel, sizeof(MqttNomeAmigavel) - 1);
  WiFiManagerParameter parameterMqttTopico("MqttTopico", "MqttTopico",   MqttTopico, sizeof(MqttTopico) - 1);

  wfm.addParameter(&parameterHost);
  wfm.addParameter(&parameterIPMqtt);
  wfm.addParameter(&parameterPorta);
  wfm.addParameter(&parameterMqttuser);
  wfm.addParameter(&parameterMqttpass);
  wfm.addParameter(&parameterMqttNomeAmigavel);
  wfm.addParameter(&parameterMqttTopico);

  strcpy(ssidX, ssid);
  strcat(ssidX, "-");
  WiFi.macAddress().toCharArray(tmp, 9);
  strcat(ssidX, tmp);

  WiFi.mode(WIFI_STA); // explicitly set mode, esp defaults to STA+AP
  // it is a good practice to make sure your code sets wifi mode how you want it.

  wfm.setRemoveDuplicateAPs(true);
  wfm.setTimeout(60);
  // wfm.setBreakAfterConfig(true);
  wfm.setSaveConfigCallback(saveConfigCallback);
  
  if (!wfm.autoConnect(ssidX, pwd))
  {
    Serial.println("\r\nFALHOU A CONEXÃO...\r\n");
    delay(2000);
    ESP.restart();
  }
  //ESP.deepSleep(20e6, WAKE_RFCAL);

  Serial.println("\r\nCONEXÃO OK...\r\n");
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //save the custom parameters to FS
  if (shouldSaveConfig)
  {
    Serial.println("SALVANDO OS NOVOS VALORES E REINICIANDO");
    //read updated parameters
    strcpy(Host, parameterHost.getValue());
    strcpy(IPMqtt, parameterIPMqtt.getValue());
    strcpy(Porta, parameterPorta.getValue());
    strcpy(Mqttuser, parameterMqttuser.getValue());
    strcpy(Mqttpass, parameterMqttpass.getValue());
    strcpy(MqttNomeAmigavel, parameterMqttNomeAmigavel.getValue());
    strcpy(MqttTopico, parameterMqttTopico.getValue());
   
    Serial.println("VALORES DO USUARIO: ");
    Serial.println("Host : " + String(Host));
    Serial.println("IPMqtt : " + String(IPMqtt));
    Serial.println("Porta : " + String(Porta)); 
    Serial.println("Mqttuser : " + String(Mqttuser)); 
    Serial.println("Mqttpass : " + String(Mqttpass)); 
    Serial.println("MqttNomeAmigavel : " + String(MqttNomeAmigavel));
    Serial.println("MqttTopico : " + String(MqttTopico));

    //  String msg, tag, valor, Host, IP, Porta;
    gravaConfigEx(String(Host), String(IPMqtt), String(Porta), String(Mqttuser), String(Mqttpass), String(MqttNomeAmigavel), String(MqttTopico));
    
    // Tempo default 5 Minuto
    TempoDeEspera = 5L * MINUTO;
    writeFile("5", ARQ_CNF_TEMPO);
    delay(3000);
    ESP.restart();
  }
  else
    Serial.println("OS VALORES NÃO FORAM EDITADOS");
}

bool reconectaMQTT() 
{
  char IPMqttserver [255 + 1];
  int PortaMqttserver;
  char Mqttuser [64 + 1];
  char Mqttpass [64 + 1];

  if (ConexaoMQTT.connected()) 
    return true;
  
  dev.IPMqtt.toCharArray(IPMqttserver, 255);
  PortaMqttserver = dev.Porta.toInt();

  if (strlen(IPMqttserver) == 0 || PortaMqttserver == 0)
  {
    Serial.println("Falta configuração do servidor MQTT");
    delay(2000);
    //ESP.restart();
    return false;
  }
  
  dev.Mqttuser.toCharArray(Mqttuser, 64);
  dev.Mqttpass.toCharArray(Mqttpass, 64);

  ConexaoMQTT.setServer(IPMqttserver, PortaMqttserver);
  //while (!ConexaoMQTT.connected())
  if (!ConexaoMQTT.connected())
  {
    Serial.print("Conectando ao Broker MQTT: ");
    Serial.print(IPMqttserver); Serial.print(":"); Serial.print(PortaMqttserver); Serial.print("?");
    Serial.print(Mqttuser); Serial.print("@"); Serial.println(Mqttpass);
    ConexaoMQTT.connect("esp8266", Mqttuser, Mqttpass);
    //delay(300);
  }
  
  if (ConexaoMQTT.connected())
  {
    Serial.println("MQTT conectado");
    return true;
  }

  Serial.println("Falha na conexão MQTT");
  delay(2000);
  return false;
}

// http://192.168.4.1/
void handleRoot() 
{
  String msg, tag, valor;
  String index = "";
  char buf[30 + 1];
  
  
  msg = "<html><head><meta http-equiv='refresh' content='5'><title>MQTT-ENERGIA ELETRICA</title></head><body>";

  msg += "<br><br><br> *** ";
  msg += TEXTO_ABERTURA;
  msg += " v";
  msg += VERSAO;
  msg += " *** <br><br><pre>";
  msg += "HOST....: " + dev.Host;
  msg += "<br>";
  msg += "IP......: " + WiFi.localIP().toString();
  valor = WiFi.SSID();  
  msg += "<br>";
  msg += "SSID....: " + valor;

  msg += "<br><br>SERVIDOR MQTT";
  msg += "<br>";
  msg += "IP/User.: " + dev.IPMqtt + ":" + dev.Porta + "?" + dev.Mqttuser;
  msg += "<br>";
  msg += "Nome....: " + dev.MqttNomeAmigavel;
  msg += "<br>";
  msg += "Topico..: " + dev.MqttTopico;
  msg += "<br>";
  valor = UltimoValor == 0? "ON": "OFF";
  msg += "Estado..: " + valor;

  msg += "<br>";
  msg += "<br>";
  
  String x = readFile(ARQ_CNF_TEMPO);
  msg += "debug ARQ_CNF_TEMPO...: " + x;
  msg += "<br>";
  
  
  msg += "debug...: " + String(TempoDeEspera);
  msg += "<br>";

  msg += "debug...: " + String(WatchdogMQTT_Contador);
  msg += "<br>";
    
  msg += "debug...: " + String(TimeCounter);
  

  msg += "<br><br></pre>";

  for (uint8_t i=0; i<server.args(); i++)
  {
    tag   = server.argName(i);
    valor = server.arg(i);
    tag.toUpperCase();
 
    if (tag == "?" || tag == "AJUDA")
    {
      msg += "*** COMANDOS ***<br><br>";
      
      msg += " config<br>";
      msg += " configTempo<br>";
      msg += " reboot<br>";
      break; // essa é a ultima.... então deve sair do loop
    }
  }

  msg += "</body></html>";

  if (msg.length() != 0)
    index = msg + index;
 
 server.send(200, "text/html", index);
}

// http://192.168.4.1/reboot
void handleReboot() {
  server.send(200, "text/plain", "+OK Reboot");
  delay(500);
  ESP.restart();
}

void handleRestart() {
  server.send(200, "text/plain", "+OK RESTART");
  delay(500);
  wfm.resetSettings();
  ESP.restart();
}

void handleStatusJson() {
  char json[192];

  time_t now = time(nullptr);
  struct tm t;
  localtime_r(&now, &t);  // mais seguro e levemente mais eficiente

  snprintf(json, sizeof(json),
    "{\"estado\":\"%s\",\"DataHora\":\"%02d/%02d/%04d %02d:%02d:%02d\"}",
    UltimoValor == LOW ? "ON" : "OFF",
    t.tm_mday,
    t.tm_mon + 1,
    t.tm_year + 1900,
    t.tm_hour,
    t.tm_min,
    t.tm_sec
  );

  server.send(200, "application/json", json);
}

void handleStatus() {
  server.send(200, "text/plain", UltimoValor == LOW ? "ON" : "OFF");
}

// http://192.168.4.1/config?Host=HostMqttSP&IP=192.168.3.7&Porta=1883&MqttUser=mqtt&MqttPass=mqtt1966&MqttNomeAmigavel=Interruptor Geral Ibiuna&MqttTopico=InterruptorGeralIbiuna
void handleConfig()
{
  String msg, tag, valor, Host, IP, Porta, Mqttuser, Mqttpass, MqttNomeAmigavel, MqttTopico;
  int contador;

  msg = "";

  for (uint8_t i=0; i<server.args(); i++){
    tag   = server.argName(i);
    valor = server.arg(i);
    tag.toUpperCase();
    
    if (tag == "HOST")
    {
      Host = valor;
      msg += " Host:" + Host;
      Serial.println(msg);
    }
    else if (tag == "IP")
    {
      IP = valor;
      msg += " IP:" + IP;
      Serial.println(msg);
    }
    else if (tag == "PORTA")
    {
      Porta = valor;
      msg += " Porta:" + Porta;
      Serial.println(msg);
    }
    else if (tag == "MQTTUSER")
    {
      Mqttuser = valor;
      msg += " Mqttuser:" + Mqttuser;
      Serial.println(msg);
    }
    else if (tag == "MQTTPASS")
    {
      Mqttpass = valor;
      msg += " Mqttpass:" + Mqttpass;
      Serial.println(msg);
    }
    else if (tag == "MQTTNOMEAMIGAVEL")
    {
      MqttNomeAmigavel = valor;
      msg += " MqttNomeAmigavel:" + MqttNomeAmigavel;
      Serial.println(msg);
    }
    else if (tag == "MQTTTOPICO")
    {
      MqttTopico = valor;
      msg += " MqttTopico:" + MqttTopico;
      Serial.println(msg);
      break; // essa é a ultima.... então deve sair do loop
    }
  }

  if (Host.length() != 0 && IP.length() != 0 && Porta.length() != 0 && Mqttuser.length() != 0 && Mqttpass.length() != 0&& MqttNomeAmigavel.length() != 0 && MqttTopico.length() != 0)
  {
    Serial.println(Host);
    Serial.println(IP);
    Serial.println(Porta);
    Serial.println(Mqttuser);
    Serial.println(Mqttpass);
    Serial.println(MqttNomeAmigavel);
    Serial.println(MqttTopico);
    
    gravaConfigEx(Host, IP, Porta, Mqttuser, Mqttpass, MqttNomeAmigavel, MqttTopico);

    server.send(200, "text/plain", "+OK Config-X" + msg);

    Serial.println(" ");
    Serial.println("reboot");

    delay(1000);
    ESP.reset();
  }
  else
    server.send(200, "text/plain", "-ERR Config-X");  
}

bool writeFile(String state, String path)
{
  bool bStt;
  // Abre o arquivo para escrita ("w" write)
  // Sobreescreve o conteúdo do arquivo
  File rFile = SPIFFS.open(path, "w+"); 
  if(!rFile)
    bStt = false;
  else
  {
    rFile.println(state);
    bStt = true;
  }
  rFile.close();
  return bStt;
}
 
String readFile(String path)
{
  String content = "";
  
  File rFile = SPIFFS.open(path, "r");
  if (!rFile)
    return content; // Erro ao abrir arquivo

  content = rFile.readStringUntil('\r'); // desconsidera '\r\n'
  rFile.close();
  return content;
}

bool appendFile(String state, String path)
{
  bool bStt;
  // Abre o arquivo para escrita ("a" adiciona no final do arquivo)
  // acrescenta ao conteúdo do arquivo
  File rFile = SPIFFS.open(path, "a+"); 
  if(!rFile)
    bStt = false;
  else
  {
    rFile.println(state);
    bStt = true;
  }
  rFile.close();
  return bStt;
}

bool openFS(void)
{ // Abre o sistema de arquivos
  if(!SPIFFS.begin())
    return false;
  else
    return true;
}

void carregaConfigEx()
{
  if(SPIFFS.exists(ARQ_SENSOR))
  {
    File rFile = SPIFFS.open(ARQ_SENSOR, "r");
    if (!rFile)
      return; // Erro ao abrir arquivo

    dev.Host             = rFile.readStringUntil('\r\n'); 
    dev.IPMqtt           = rFile.readStringUntil('\r\n');
    dev.Porta            = rFile.readStringUntil('\r\n');
    dev.Mqttuser         = rFile.readStringUntil('\r\n');
    dev.Mqttpass         = rFile.readStringUntil('\r\n');
    dev.MqttNomeAmigavel = rFile.readStringUntil('\r\n');
    dev.MqttTopico       = rFile.readStringUntil('\r\n');

    dev.Host.trim(); 
    dev.IPMqtt.trim();
    dev.Porta.trim();
    dev.Mqttuser.trim();
    dev.Mqttpass.trim();
    dev.MqttNomeAmigavel.trim();
    dev.MqttTopico.trim();

    Serial.println("");
    Serial.println("CONFIG DO SENSOR");
    Serial.println("");

    Serial.print("dev.Host..............: ");  Serial.println(dev.Host);
    Serial.print("dev.IPMqtt............: ");  Serial.println(dev.IPMqtt);
    Serial.print("dev.Porta.............: ");  Serial.println(dev.Porta);
    Serial.print("dev.Mqttuser..........: ");  Serial.println(dev.Mqttuser);
    Serial.print("dev.Mqttpass..........: ");  Serial.println(dev.Mqttpass);
    Serial.print("dev.MqttNomeAmigavel..: ");  Serial.println(dev.MqttNomeAmigavel);
    Serial.print("dev.MqttTopico........: ");  Serial.println(dev.MqttTopico);

    //nome   = dev.MqttNomeAmigavel;
    String topico = "esp8266/" + dev.MqttTopico;

    Serial.print("dev.MqttTopicoCompleto: ");  Serial.println(topico);

    Serial.println("");

    rFile.close();
    Ativo = true;
  }
  else
  {
    Serial.println("");
    Serial.println("FALHA NO CONFIG DO SENSOR");
    Serial.println("");

    dev.Host             = ""; 
    dev.IPMqtt           = "";
    dev.Porta            = "";
    dev.Mqttuser         = "";
    dev.Mqttpass         = "";
    dev.MqttNomeAmigavel = "";
    dev.MqttTopico       = "";
    
    Ativo = false;
  }  
}
/*
    dev.Host             = a; 
    dev.IPMqtt           = b;
    dev.Porta            = c;
    dev.Mqttuser         = d;
    dev.Mqttpass         = e;
    dev.MqttNomeAmigavel = f;
    dev.MqttTopico       = g;
 */
bool gravaConfigEx(String a, String b, String c, String d, String e, String f, String g)
{  
  bool bStt;
  
  if ((bStt = writeFile(a, ARQ_TEMP_SENSOR)) == true) // Reinicia o arquivo
    if ((bStt = appendFile(b, ARQ_TEMP_SENSOR)) == true) // Adiciona linha ao arquivo
      if ((bStt = appendFile(c, ARQ_TEMP_SENSOR)) == true) // Adiciona linha ao arquivo
        if ((bStt = appendFile(d, ARQ_TEMP_SENSOR)) == true) // Adiciona linha ao arquivo
          if ((bStt = appendFile(e, ARQ_TEMP_SENSOR)) == true) // Adiciona linha ao arquivo
            if ((bStt = appendFile(f, ARQ_TEMP_SENSOR)) == true) // Adiciona linha ao arquivo
              bStt = appendFile(g, ARQ_TEMP_SENSOR);               // Adiciona linha ao arquivo

  if (bStt)
  {
    SPIFFS.remove(ARQ_SENSOR);
    SPIFFS.rename(ARQ_TEMP_SENSOR, ARQ_SENSOR);
  }
      
  return bStt;
}

void WatchdogMQTT()
{
  int iRst;
  unsigned long actualMillis = millis();

  
  if (!Ativo) return;
  if (actualMillis - lastChange < TempoDeEspera) return;         // Aguarda o tempo necessário antes de enviar sonda

  if (bPrimeiraVez)
  {
     bPrimeiraVez = false;
     TempoDeEspera = 5L * MINUTO;
     String x = readFile(ARQ_CNF_TEMPO);
     TempoDeEspera = x.toInt();
     if (TempoDeEspera > 60)
     {
        TempoDeEspera = 15;
        writeFile("15", ARQ_CNF_TEMPO);
     }
     else if (TempoDeEspera <= 1)
     {
       TempoDeEspera = 5;
       writeFile("5", ARQ_CNF_TEMPO);
     }

     TempoDeEspera = TempoDeEspera * MINUTO;
  }

  Serial.println("++++++++++++++++++++ WatchdogMQTT() ++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
  Serial.print("TempoDeEspera = ");
  Serial.println(TempoDeEspera / MINUTO);

  reconectaMQTT();  
  RegistraSensor();
  
  lastChange = actualMillis; // update debounce time
  WatchdogMQTT_Contador++;
  
  EnviaEstadoMQTT(digitalRead(PIN_BOTAO) == 0? true: false, false);

  return;
}

// Configura Sensores através do MQTT Discovery do Home Assistant
void RegistraSensor()
{
  String topico = "esp8266/" + dev.MqttTopico;
  
  String novosensor = "{\"expire_after\": \"600\", \"icon\": \"mdi:gesture-tap-button\", \"name\": \"" + dev.MqttNomeAmigavel + "\", \"state_topic\": \"" + topico + "/state\"}";
  String cnf = "homeassistant/binary_sensor/" + topico + "/config";

  Serial.println(novosensor);
  Serial.println(cnf);
  
  ConexaoMQTT.publish(cnf.c_str(), novosensor.c_str(), false);
}

bool EnviaEstadoMQTT(bool bEstado, bool bDbu)
{
  #define PAYLOAD_ON   "ON"
  #define PAYLOAD_OFF  "OFF"
  String topicoLocal = "esp8266/" + dev.MqttTopico + "/state";

  reconectaMQTT();

  if (bEstado)
  {
    if (bDbu) Serial.println(PAYLOAD_ON);
    ConexaoMQTT.publish(topicoLocal.c_str(), PAYLOAD_ON);
    digitalWrite(PIN_LED, HIGH);
    bEstado = false;
  }
  else
  {
    if (bDbu) Serial.println(PAYLOAD_OFF);
    ConexaoMQTT.publish(topicoLocal.c_str(), PAYLOAD_OFF);
    digitalWrite(PIN_LED, LOW);
    bEstado = true;
  }
  
  return bEstado;
}

void sendMessage(const char *MESSAGE)
{
  String MSG = MESSAGE;
  time_t now = time(nullptr);

  String payload = "{";
         payload += "\"msg\":\"" + MSG + "\",";
         payload += "\"source\":\"esp8266\",";
         payload += "\"device\":\"sensor_sala\",";
         payload += "\"ip\":\"" + WiFi.localIP().toString() + "\",";
         payload += "\"ts\":\"" + String(now) + "\"";
         payload += "}";

  ConexaoMQTT.publish("instagram/send", payload.c_str());
}

//////////////////////////////////////////////////////////////  SETUP
void setup() 
{
  Serial.begin(BAUD_RATE);

  openFS();

  // SPIFFS.format();
  
  carregaConfigEx();
  setupWiFiAuto();

  // https://dl.dropboxusercontent.com/s/4e2puj5noup64jt/testeURL.txt
  server.on("/", handleRoot);
  //Envia o arquivo html quando entrar quando fizerem uma requisição na raiz do servidor
  //server.serveStatic("/", SPIFFS, "/automacao.html");
  server.on("/reboot", handleReboot);
  server.on("/config", handleConfig);

  server.on("/status", handleStatus);
  server.on("/status/json", handleStatusJson);
  server.on("/restart", handleRestart);


  //listSSIDex();
  //while(1) server.handleClient();

  MDNS.begin(dev.Host);

  // Inicia o servidor WEB
  atualizadorOTA.setup(&server);
  server.begin();

  MDNS.addService("http", "tcp", 80);
  
  Serial.println("Server iniciado");

  Serial.printf("HTTPUpdateServer pronto! Use: http://%s.local/update no seu browser\n", dev.Host);
  
  
  Serial.print("\r\n"); Serial.print(TEXTO_ABERTURA); Serial.print(" - v"); Serial.print(VERSAO); Serial.println("\r\n\r\n");

  Serial.print("SSID............: "); Serial.println(WiFi.SSID());
  Serial.print("Endereço de IP..: "); Serial.println(WiFi.localIP());
  Serial.print("MAC.............: "); Serial.println(WiFi.macAddress());

  //wfm.resetSettings();
  //ESP.restart();
  
  reconectaMQTT();
  RegistraSensor();
  
  //pinMode(PIN_BOTAO, INPUT_PULLUP); 
  pinMode(PIN_BOTAO, INPUT); 
  pinMode(PIN_LED, OUTPUT);

  // Primeira leitura e envio
  // EnviaEstadoMQTT(digitalRead(PIN_BOTAO) == 0? true: false, false);

  configTime(UTC_OFFSET, UTC_OFFSET_DST, NTP_SERVER);

  Serial.println("\r\nOK\r\n\r\n");
  writeFile("2", ARQ_CNF_TEMPO);

  String msg = dev.MqttNomeAmigavel + ": " + WiFi.localIP().toString();
  sendMessage(msg.c_str());

}


//////////////////////////////////////////////////////////////  LOOP
void loop() 
{
  server.handleClient();
  MDNS.update();
 
  WatchdogMQTT();

  int LeituraSwitchD1Pin = digitalRead(PIN_BOTAO);

  if (LeituraSwitchD1Pin != UltimoValor) 
  {
    EnviaEstadoMQTT(LeituraSwitchD1Pin == 0? true: false, false);
    UltimoValor = LeituraSwitchD1Pin;
  }

  if (TimeCounter < 300) 
    TimeCounter++;
  else 
  {
    Serial.println("TimeCounter automatico");
    EnviaEstadoMQTT(LeituraSwitchD1Pin == 0? true: false, false);
    TimeCounter = 0;
    UltimoValor = LeituraSwitchD1Pin;
  }
  delay(300);
}
