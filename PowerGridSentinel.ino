// "Ad Astra Per Aspera"
// "Por asperos caminhos até as Estrelas"
// "Para as estrelas através das dificuldades"

/*

  456
  123
  789

 */

// =====================================================================
// Power Grid Sentinel - sensor de energia da rede elétrica (ESP8266 / ESP-01)
// Compatível com Home Assistant como se fosse um Tasmota.
//
// O sensor (pino PIN_BOTAO, nível LOW = ON) é publicado como um "Switch" do
// Tasmota em SwitchMode 2 (follow invertido). A integração "Tasmota" do
// Home Assistant cria o dispositivo e o binary_sensor sozinha.
//
// MQTT (Tasmota):   <topico> = campo "MqttTopico" do portal de configuração
//   cmnd/<topico>/STATE            publica tele/<topico>/STATE
//   cmnd/<topico>/STATUS [0|2|5|8|10|11]
//   cmnd/<topico>/TELEPERIOD [10..3600]   (o aparelho trabalha em minutos: 2 a 60)
//   cmnd/<topico>/RESTART 1
//   cmnd/tasmotas/<comando>        tópico de grupo (todos os aparelhos)
//   stat/<topico>/RESULT           {"Switch1":{"Action":"ON"}}  a cada mudança
//   stat/<topico>/STATUS<n>        resposta do STATUS
//   tele/<topico>/LWT              Online / Offline (retido)
//   tele/<topico>/SENSOR           {"Time":"...","Switch1":"ON"}
//   tele/<topico>/STATE            JSON de estado (a cada TempoDeEspera)
//   tasmota/discovery/<MAC>/config descoberta automática no Home Assistant
//
// MQTT (legado, mantido): esp8266/<topico>/state  ON / OFF
//
// HTTP: /  /?ajuda  /status  /status/json  /config  /configTempo  /reboot
//       /restart  /update (OTA)
//       /cm?cmnd=Status%2010   (API de comandos igual à do Tasmota)
// =====================================================================

#include <Arduino.h>

#ifndef ESP8266
  #error "Este sketch é para ESP8266 (ESP-01 / NodeMCU / ESP8266_GENERIC)."
#endif

#include <ESP8266WiFi.h>
#include <ESP8266WebServer.h>
#include <ESP8266HTTPUpdateServer.h> //  Biblioteca que cria o servico de atualizacão via wifi (ou Over The Air - OTA)
#include <ESP8266mDNS.h>
#include <WiFiManager.h>             //  https://github.com/tzapu/WiFiManager
#include <PubSubClient.h>            //  versão 2.8 ou mais nova (setBufferSize)
#include <FS.h>
#include <time.h>

// Constantes
#define BAUD_RATE         115200

#define NTP_SERVER     "pool.ntp.org"
#define UTC_OFFSET     (-3 * 3600)
#define UTC_OFFSET_DST 0

#define SEGUNDO           (1000UL)
#define MINUTO            (60UL * SEGUNDO)
#define MEIA_HORA         (30UL * MINUTO)
#define HORA              (60UL * MINUTO)

// configuração salva
#define ARQ_CNF           "/arquivo.conf"
#define ARQ_CNF_TEMPO     "/tempo.conf"
#define ARQ_SENSOR        "/sensor.conf"
#define ARQ_TEMP_SENSOR   "/sensor.temp"


#define TEXTO_ABERTURA    "Power Grid Sentinel"
#define VERSAO            "1.05"
#define HOST_NOME         "ESP8266-PowerGridSentinel"
#define SSID_NOME         "PowerGridSentinel"
#define PASS_NOME         ""

// =====================================================
// HOME ASSISTANT / TASMOTA
// =====================================================

// 0 = sem discovery
// 1 = discovery nativo do Tasmota (integração "Tasmota" do Home Assistant)
// 2 = MQTT Discovery padrão do HA (binary_sensor lendo tele/<topico>/SENSOR)
// 3 = discovery original deste aparelho (esp8266/<topico>, expire_after 600)
#define DISCOVERY_MODO          1

// mantém o tópico antigo esp8266/<topico>/state (ON / OFF)
#define MQTT_LEGADO             1
#define MQTT_LEGADO_RETIDO      0       // 0 = igual ao original (não retido)

// nos modos 1 e 2, apaga do broker a entidade antiga
// homeassistant/binary_sensor/esp8266/<topico>/config (evita entidade duplicada no HA)
#define LIMPA_DISCOVERY_LEGADO  1

#define TOPICO_GRUPO            "tasmotas"
#define MQTT_BUFFER             1536

#define TEMPO_PADRAO_MIN        2       // minutos entre as publicações periódicas (TelePeriod = 120 s)
#define LEITURA_MS              300UL   // intervalo de leitura do sensor
#define ESTADO_PERIODICO_LEITURAS 300   // 300 leituras x 300 ms = republica o estado a cada ~90 s

#if DISCOVERY_MODO < 0 || DISCOVERY_MODO > 3
  #error "DISCOVERY_MODO deve ser 0, 1, 2 ou 3"
#endif

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
unsigned long TempoDeEspera = TEMPO_PADRAO_MIN * MINUTO;
bool shouldSaveConfig = false;  // flag for saving data
int UltimoValor = 0xff;  // valor deve ser diferente de 0 e 1
int TimeCounter = 0;

unsigned long ultimaTentativaMqtt = 0;
unsigned long intervaloMqtt       = 0;
unsigned long reiniciarEm         = 0;
uint32_t      contadorMqtt        = 0;

bool reiniciarPendente     = false;
bool resetarWifiPendente   = false;
bool cmdPendente           = false;
bool haOnlinePendente      = false;
bool mensagemBootPendente  = true;

String cmdPend;
String argPend;


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

// tópicos e identificadores (montados em montaTopicos)
String tCmnd;      // cmnd/<topico>/
String tStat;      // stat/<topico>/
String tTele;      // tele/<topico>/
String tGrupo;     // cmnd/tasmotas/
String tLegado;    // esp8266/<topico>
String idMac;      // AABBCCDDEEFF
String idCliente;  // esp8266-<chipid>


// Objetos
ESP8266HTTPUpdateServer atualizadorOTA; //Este e o objeto que permite atualizacao do programa via wifi (OTA)
ESP8266WebServer server(80);
WiFiManager wfm;


//Variáveis
WiFiClient wifiClient;
PubSubClient ConexaoMQTT(wifiClient);


// =====================================================
// UTILITÁRIOS
// =====================================================

String jsonEsc(const String &in)
{
  String o;
  o.reserve(in.length() + 4);

  for (unsigned int i = 0; i < in.length(); i++)
  {
    char c = in[i];

    if (c == '"' || c == '\\')
    {
      o += '\\';
      o += c;
    }
    else if ((unsigned char)c < 0x20)
      o += ' ';
    else
      o += c;
  }

  return o;
}

String htmlEsc(String s)
{
  s.replace("&", "&amp;");
  s.replace("<", "&lt;");
  s.replace(">", "&gt;");
  return s;
}

// '/', '#' e '+' quebram o tópico MQTT. Espaços são mantidos para não mudar tópicos já em uso.
String sanitizaTopico(String s)
{
  s.trim();

  for (unsigned int i = 0; i < s.length(); i++)
  {
    char c = s[i];

    if (c == '/' || c == '#' || c == '+' || (unsigned char)c < 0x20)
      s.setCharAt(i, '_');
  }

  return s;
}

bool ehNumero(const String &s)
{
  if (!s.length())
    return false;

  for (unsigned int i = 0; i < s.length(); i++)
    if (s[i] < '0' || s[i] > '9')
      return false;

  return true;
}

void repete(String &s, const char *item, uint8_t n)
{
  while (n--)
    s += item;
}

// remove as chaves externas: {"A":1} -> "A":1
String semChaves(const String &s)
{
  if (s.length() < 2)
    return "";

  return s.substring(1, s.length() - 1);
}

// tempo ligado em ms sem estourar a cada 49 dias (precisa ser chamado no loop)
uint64_t uptimeMs()
{
  static unsigned long anterior = 0;
  static uint64_t acumulado = 0;

  unsigned long agora = millis();
  acumulado += (unsigned long)(agora - anterior);
  anterior = agora;

  return acumulado;
}

String textoUptime()
{
  unsigned long s = (unsigned long)(uptimeMs() / 1000ULL);
  char b[24];

  snprintf(b, sizeof(b), "%luT%02lu:%02lu:%02lu", s / 86400UL, (s / 3600UL) % 24UL, (s / 60UL) % 60UL, s % 60UL);

  return String(b);
}

String textoHoraISO()
{
  time_t now = time(nullptr);
  struct tm t;
  char b[24];

  localtime_r(&now, &t);
  strftime(b, sizeof(b), "%Y-%m-%dT%H:%M:%S", &t);

  return String(b);
}

String textoDataHoraBR()
{
  time_t now = time(nullptr);
  struct tm t;
  char b[48];

  localtime_r(&now, &t);

  snprintf(b, sizeof(b), "%02d/%02d/%04d %02d:%02d:%02d",
    t.tm_mday,
    t.tm_mon + 1,
    t.tm_year + 1900,
    t.tm_hour,
    t.tm_min,
    t.tm_sec
  );

  return String(b);
}

const char *modoWifi()
{
  switch (WiFi.getPhyMode())
  {
    case WIFI_PHY_MODE_11B: return "11b";
    case WIFI_PHY_MODE_11G: return "11g";
    default:                return "11n";
  }
}

// =====================================================
// SENSOR / LED
// =====================================================

// nível LOW no pino = energia presente = ON
bool entradaLigada()
{
  return UltimoValor == LOW;
}

const char *textoEstado()
{
  return entradaLigada() ? "ON" : "OFF";
}

void atualizaLed()
{
  digitalWrite(PIN_LED, entradaLigada() ? HIGH : LOW);
}

//callback notifying us of the need to save config
void saveConfigCallback ()
{
  Serial.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Should save config");
  shouldSaveConfig =   true;
}

// =====================================================
// FS
// =====================================================

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

// =====================================================
// CONFIG
// =====================================================

void carregaConfigEx()
{
  if(SPIFFS.exists(ARQ_SENSOR))
  {
    File rFile = SPIFFS.open(ARQ_SENSOR, "r");
    if (!rFile)
      return; // Erro ao abrir arquivo

    dev.Host             = rFile.readStringUntil('\n');
    dev.IPMqtt           = rFile.readStringUntil('\n');
    dev.Porta            = rFile.readStringUntil('\n');
    dev.Mqttuser         = rFile.readStringUntil('\n');
    dev.Mqttpass         = rFile.readStringUntil('\n');
    dev.MqttNomeAmigavel = rFile.readStringUntil('\n');
    dev.MqttTopico       = rFile.readStringUntil('\n');

    dev.Host.trim();
    dev.IPMqtt.trim();
    dev.Porta.trim();
    dev.Mqttuser.trim();
    dev.Mqttpass.trim();
    dev.MqttNomeAmigavel.trim();
    dev.MqttTopico = sanitizaTopico(dev.MqttTopico);

    Serial.println("");
    Serial.println("CONFIG DO SENSOR");
    Serial.println("");

    Serial.print("dev.Host..............: ");  Serial.println(dev.Host);
    Serial.print("dev.IPMqtt............: ");  Serial.println(dev.IPMqtt);
    Serial.print("dev.Porta.............: ");  Serial.println(dev.Porta);
    Serial.print("dev.Mqttuser..........: ");  Serial.println(dev.Mqttuser);
    Serial.print("dev.Mqttpass..........: ");  Serial.println(dev.Mqttpass.length() ? "(definida)" : "(vazia)");
    Serial.print("dev.MqttNomeAmigavel..: ");  Serial.println(dev.MqttNomeAmigavel);
    Serial.print("dev.MqttTopico........: ");  Serial.println(dev.MqttTopico);
    Serial.print("dev.MqttTopicoCompleto: ");  Serial.println("esp8266/" + dev.MqttTopico);

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

// Intervalo (em minutos) entre as publicações periódicas, guardado em ARQ_CNF_TEMPO.
// Faixa válida: 2 a 60. Valores fora da faixa em arquivo existente seguem a regra original
// (acima de 60 vira 15; 1 ou menos vira 5). Antes, o setup() regravava "2" a cada boot,
// então o valor configurado nunca era mantido; agora o arquivo só é criado se não existir.
void carregaTempo()
{
  long minutos;

  if (!SPIFFS.exists(ARQ_CNF_TEMPO))
  {
    minutos = TEMPO_PADRAO_MIN;
    writeFile(String(minutos), ARQ_CNF_TEMPO);
  }
  else
  {
    String x = readFile(ARQ_CNF_TEMPO);
    x.trim();
    minutos = x.toInt();

    if (minutos > 60)
    {
      minutos = 15;
      writeFile("15", ARQ_CNF_TEMPO);
    }
    else if (minutos <= 1)
    {
      minutos = 5;
      writeFile("5", ARQ_CNF_TEMPO);
    }
  }

  TempoDeEspera = (unsigned long)minutos * MINUTO;
}

bool defineTempoMin(long minutos)
{
  if (minutos < 2 || minutos > 60)
    return false;

  TempoDeEspera = (unsigned long)minutos * MINUTO;
  writeFile(String(minutos), ARQ_CNF_TEMPO);

  return true;
}

void montaTopicos()
{
  char cid[24];

  idMac = WiFi.macAddress();
  idMac.replace(":", "");
  idMac.toUpperCase();

  snprintf(cid, sizeof(cid), "esp8266-%u", (unsigned int)ESP.getChipId());
  idCliente = cid;

  tCmnd   = String("cmnd/") + dev.MqttTopico + "/";
  tStat   = String("stat/") + dev.MqttTopico + "/";
  tTele   = String("tele/") + dev.MqttTopico + "/";
  tGrupo  = String("cmnd/" TOPICO_GRUPO "/");
  tLegado = String("esp8266/") + dev.MqttTopico;
}

// =====================================================
// WIFI
// =====================================================

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
  if (dev.Host.length() != 0)      dev.Host.toCharArray(Host, sizeof(Host));
  if (dev.IPMqtt.length() != 0)    dev.IPMqtt.toCharArray(IPMqtt, sizeof(IPMqtt));
  if (dev.Porta.length() != 0)     dev.Porta.toCharArray(Porta, sizeof(Porta));
  if (dev.Mqttuser.length() != 0)  dev.Mqttuser.toCharArray(Mqttuser, sizeof(Mqttuser));
  if (dev.Mqttpass.length() != 0)  dev.Mqttpass.toCharArray(Mqttpass, sizeof(Mqttpass));
  if (dev.MqttNomeAmigavel.length() != 0)  dev.MqttNomeAmigavel.toCharArray(MqttNomeAmigavel, sizeof(MqttNomeAmigavel));
  if (dev.MqttTopico.length() != 0)        dev.MqttTopico.toCharArray(MqttTopico, sizeof(MqttTopico));

  // static: o WiFiManager guarda ponteiros para estes objetos
  static WiFiManagerParameter parameterHost("host",   "Host",    Host,  sizeof(Host) - 1);
  static WiFiManagerParameter parameterIPMqtt("IPMqtt",  "IPMqtt", IPMqtt, sizeof(IPMqtt) - 1);
  static WiFiManagerParameter parameterPorta("Porta", "Porta",   Porta, sizeof(Porta) - 1);
  static WiFiManagerParameter parameterMqttuser("Mqttuser", "Mqttuser",   Mqttuser, sizeof(Mqttuser) - 1);
  static WiFiManagerParameter parameterMqttpass("Mqttpass", "Mqttpass",   Mqttpass, sizeof(Mqttpass) - 1);
  static WiFiManagerParameter parameterMqttNomeAmigavel("MqttNomeAmigavel", "MqttNomeAmigavel",   MqttNomeAmigavel, sizeof(MqttNomeAmigavel) - 1);
  static WiFiManagerParameter parameterMqttTopico("MqttTopico", "MqttTopico",   MqttTopico, sizeof(MqttTopico) - 1);

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

    String h  = parameterHost.getValue();
    String ip = parameterIPMqtt.getValue();
    String po = parameterPorta.getValue();
    String us = parameterMqttuser.getValue();
    String pw = parameterMqttpass.getValue();
    String nm = parameterMqttNomeAmigavel.getValue();
    String tp = sanitizaTopico(parameterMqttTopico.getValue());

    Serial.println("VALORES DO USUARIO: ");
    Serial.println("Host : " + h);
    Serial.println("IPMqtt : " + ip);
    Serial.println("Porta : " + po);
    Serial.println("Mqttuser : " + us);
    Serial.println("MqttNomeAmigavel : " + nm);
    Serial.println("MqttTopico : " + tp);

    gravaConfigEx(h, ip, po, us, pw, nm, tp);

    // Tempo default (o setup() original regravava "2" a cada boot, então na prática era 2 minutos)
    TempoDeEspera = TEMPO_PADRAO_MIN * MINUTO;
    writeFile(String(TEMPO_PADRAO_MIN), ARQ_CNF_TEMPO);
    delay(3000);
    ESP.restart();
  }
  else
    Serial.println("OS VALORES NÃO FORAM EDITADOS");
}

// =====================================================
// MQTT - PUBLICAÇÃO
// =====================================================

bool mqttPub(const String &topico, const String &payload, bool retido)
{
  if (!ConexaoMQTT.connected())
    return false;

  return ConexaoMQTT.publish(topico.c_str(), payload.c_str(), retido);
}

// "Time":"...","Switch1":"ON"   (sem as chaves externas)
String corpoSwitch()
{
  String s = F("\"Time\":\"");
  s += textoHoraISO();
  s += F("\",\"Switch1\":\"");
  s += textoEstado();
  s += F("\"");

  return s;
}

String jsonSensor()
{
  return String("{") + corpoSwitch() + "}";
}

// {"Switch1":{"Action":"ON"}}  igual ao Tasmota com SetOption114
String jsonAcao()
{
  String s = F("{\"Switch1\":{\"Action\":\"");
  s += textoEstado();
  s += F("\"}}");

  return s;
}

String jsonEstado()
{
  int rssi = WiFi.RSSI();
  int qualidade = 2 * (rssi + 100);   // Tasmota mostra RSSI em % e Signal em dBm

  if (qualidade < 0)   qualidade = 0;
  if (qualidade > 100) qualidade = 100;

  String s;
  s.reserve(420);

  s  = F("{\"Time\":\"");            s += textoHoraISO();
  s += F("\",\"Uptime\":\"");        s += textoUptime();
  s += F("\",\"UptimeSec\":");       s += (unsigned long)(uptimeMs() / 1000ULL);
  s += F(",\"Heap\":");              s += (unsigned long)(ESP.getFreeHeap() / 1024UL);
  s += F(",\"MqttCount\":");         s += (unsigned long)contadorMqtt;
  s += F(",\"Wifi\":{\"AP\":1,\"SSId\":\"");  s += jsonEsc(WiFi.SSID());
  s += F("\",\"BSSId\":\"");         s += WiFi.BSSIDstr();
  s += F("\",\"Channel\":");         s += (long)WiFi.channel();
  s += F(",\"Mode\":\"");            s += modoWifi();
  s += F("\",\"RSSI\":");            s += qualidade;
  s += F(",\"Signal\":");            s += rssi;
  s += F(",\"LinkCount\":1,\"Downtime\":\"0T00:00:00\"}}");

  return s;
}

// Publica o estado do sensor. mudou = true quando o nível do pino acabou de mudar.
// (antes: EnviaEstadoMQTT(bool, bool) — bloqueava com delay() para reconectar)
void EnviaEstadoMQTT(bool mudou)
{
  atualizaLed();

#if MQTT_LEGADO
  mqttPub(tLegado + "/state", textoEstado(), MQTT_LEGADO_RETIDO);
#endif

  mqttPub(tTele + "SENSOR", jsonSensor(), false);

  if (mudou)
    mqttPub(tStat + "RESULT", jsonAcao(), false);
}

void publicaTele()
{
  mqttPub(tTele + "STATE", jsonEstado(), false);
}

void publicaInfo()
{
  String s;

  s  = F("{\"Info1\":{\"Module\":\"" TEXTO_ABERTURA " ESP8266\",\"Version\":\"" VERSAO "\",\"FallbackTopic\":\"cmnd/");
  s += idCliente;
  s += F("_fb/\",\"GroupTopic\":\"cmnd/" TOPICO_GRUPO "/\"}}");
  mqttPub(tTele + "INFO1", s, false);

  s  = F("{\"Info2\":{\"WebServerMode\":\"Admin\",\"Hostname\":\"");
  s += jsonEsc(dev.Host);
  s += F("\",\"IPAddress\":\"");
  s += WiFi.localIP().toString();
  s += F("\"}}");
  mqttPub(tTele + "INFO2", s, false);

  s  = F("{\"Info3\":{\"RestartReason\":\"");
  s += jsonEsc(ESP.getResetReason());
  s += F("\"}}");
  mqttPub(tTele + "INFO3", s, false);
}

// =====================================================
// DISCOVERY (Home Assistant)
// =====================================================

#if DISCOVERY_MODO == 1
// Formato idêntico ao do firmware Tasmota (xdrv_12_discovery.ino).
// swc = [2, -1, ...]: Switch1 em SwitchMode 2 (follow invertido: LOW = ON), os demais não usados.
// so "114" = 1: o Switch envia {"Switch1":{"Action":"ON"}} em stat/<topico>/RESULT.
void publicaDiscoveryTasmota()
{
  String nome = jsonEsc(dev.MqttNomeAmigavel);
  String j;
  j.reserve(1000);

  j  = F("{\"ip\":\"");              j += WiFi.localIP().toString();
  j += F("\",\"dn\":\"");            j += nome;
  j += F("\",\"fn\":[\"");           j += nome; j += '"'; repete(j, ",null", 7);
  j += F("],\"hn\":\"");             j += jsonEsc(dev.Host);
  j += F("\",\"mac\":\"");           j += idMac;
  j += F("\",\"md\":\"");            j += TEXTO_ABERTURA; j += F(" ESP8266");
  j += F("\",\"ty\":0,\"if\":0,\"cam\":0,\"ofln\":\"Offline\",\"onln\":\"Online\"");
  j += F(",\"state\":[\"OFF\",\"ON\",\"TOGGLE\",\"HOLD\"],\"sw\":\"");  j += VERSAO;
  j += F("\",\"t\":\"");             j += jsonEsc(dev.MqttTopico);
  j += F("\",\"ft\":\"%prefix%/%topic%/\",\"tp\":[\"cmnd\",\"stat\",\"tele\"]");
  j += F(",\"rl\":[0");              repete(j, ",0", 7);
  j += F("],\"swc\":[2");            repete(j, ",-1", 7);
  j += F("],\"swn\":[null");         repete(j, ",null", 7);
  j += F("],\"btn\":[0");            repete(j, ",0", 7);
  j += F("],\"so\":{\"4\":0,\"11\":0,\"13\":0,\"17\":0,\"20\":0,\"30\":0,\"68\":0,\"73\":0,\"82\":0,\"114\":1,\"117\":0}");
  j += F(",\"lk\":1,\"lt_st\":0,\"bat\":0,\"dslp\":0,\"sho\":[],\"sht\":[],\"ver\":1}");

  mqttPub(String("tasmota/discovery/") + idMac + "/config", j, true);

  String s = F("{\"sn\":{\"Time\":\"");
  s += textoHoraISO();
  s += F("\"},\"ver\":1}");

  mqttPub(String("tasmota/discovery/") + idMac + "/sensors", s, true);
}
#endif

#if DISCOVERY_MODO == 2
// MQTT Discovery padrão do HA, sem precisar da integração Tasmota.
// O estado vem de tele/<topico>/SENSOR, publicado a cada mudança e periodicamente.
void publicaDiscoveryHA()
{
  String nome = jsonEsc(dev.MqttNomeAmigavel);
  String j;
  j.reserve(700);

  j  = F("{\"name\":\"");           j += nome;
  j += F("\",\"uniq_id\":\"");      j += idMac; j += F("_switch1");
  j += F("\",\"stat_t\":\"");       j += jsonEsc(tTele); j += F("SENSOR");
  j += F("\",\"val_tpl\":\"{{ value_json.Switch1 }}\",\"pl_on\":\"ON\",\"pl_off\":\"OFF\"");
  j += F(",\"avty_t\":\"");         j += jsonEsc(tTele); j += F("LWT");
  j += F("\",\"pl_avail\":\"Online\",\"pl_not_avail\":\"Offline\"");
  j += F(",\"icon\":\"mdi:gesture-tap-button\",\"dev\":{\"ids\":[\"");  j += idMac;
  j += F("\"],\"name\":\"");        j += nome;
  j += F("\",\"mdl\":\"" TEXTO_ABERTURA " ESP8266\",\"sw\":\"" VERSAO "\"}}");

  mqttPub(String("homeassistant/binary_sensor/") + idMac + "_switch1/config", j, true);
}
#endif

#if DISCOVERY_MODO == 3
// Discovery original deste aparelho, mantido sem alterações de formato.
void publicaDiscoveryLegado()
{
  String topico = tLegado;

  String novosensor = "{\"expire_after\": \"600\", \"icon\": \"mdi:gesture-tap-button\", \"name\": \"" + jsonEsc(dev.MqttNomeAmigavel) + "\", \"state_topic\": \"" + jsonEsc(topico) + "/state\"}";
  String cnf = "homeassistant/binary_sensor/" + topico + "/config";

  Serial.println(novosensor);
  Serial.println(cnf);

  mqttPub(cnf, novosensor, false);
}
#endif

// Configura Sensores através do MQTT Discovery do Home Assistant
// limparLegado = true apaga a entidade antiga do broker (só na conexão, não a cada watchdog)
void RegistraSensor(bool limparLegado)
{
#if DISCOVERY_MODO == 1 || DISCOVERY_MODO == 2
  #if LIMPA_DISCOVERY_LEGADO
    // payload vazio + retido = apaga a entidade antiga no broker
    if (limparLegado)
      mqttPub(String("homeassistant/binary_sensor/") + tLegado + "/config", "", true);
  #endif
#endif

#if DISCOVERY_MODO == 1
  publicaDiscoveryTasmota();
#elif DISCOVERY_MODO == 2
  publicaDiscoveryHA();
#elif DISCOVERY_MODO == 3
  publicaDiscoveryLegado();
#endif
}

void sendMessage(const char *MESSAGE)
{
  String MSG = MESSAGE;
  time_t now = time(nullptr);

  String payload = "{";
         payload += "\"msg\":\"" + jsonEsc(MSG) + "\",";
         payload += "\"source\":\"esp8266\",";
         payload += "\"device\":\"sensor_sala\",";
         payload += "\"ip\":\"" + WiFi.localIP().toString() + "\",";
         payload += "\"ts\":\"" + String((unsigned long)now) + "\"";
         payload += "}";

  mqttPub("instagram/send", payload, false);
}

// =====================================================
// COMANDOS (iguais aos do Tasmota)
// =====================================================

String jsonStatus()
{
  String nome = jsonEsc(dev.MqttNomeAmigavel);
  String s;

  s  = F("{\"Status\":{\"Module\":18,\"DeviceName\":\"");  s += nome;
  s += F("\",\"FriendlyName\":[\"");                        s += nome;
  s += F("\"],\"Topic\":\"");                               s += jsonEsc(dev.MqttTopico);
  s += F("\",\"ButtonTopic\":\"0\",\"Power\":0,\"SwitchTopic\":\"0\",\"SwitchMode\":[2,0,0,0,0,0,0,0]}}");

  return s;
}

String jsonStatusFWR()
{
  String s;

  s  = F("{\"StatusFWR\":{\"Version\":\"" VERSAO "\",\"BuildDateTime\":\"" __DATE__ " " __TIME__ "\",\"Core\":\"");
  s += jsonEsc(ESP.getCoreVersion());
  s += F("\",\"SDK\":\"");
  s += jsonEsc(String(ESP.getSdkVersion()));
  s += F("\",\"Hardware\":\"ESP8266EX\"}}");

  return s;
}

String jsonStatusNET()
{
  String s;

  s  = F("{\"StatusNET\":{\"Hostname\":\"");  s += jsonEsc(dev.Host);
  s += F("\",\"IPAddress\":\"");              s += WiFi.localIP().toString();
  s += F("\",\"Gateway\":\"");                s += WiFi.gatewayIP().toString();
  s += F("\",\"Subnetmask\":\"");             s += WiFi.subnetMask().toString();
  s += F("\",\"DNSServer\":\"");              s += WiFi.dnsIP().toString();
  s += F("\",\"Mac\":\"");                    s += WiFi.macAddress();
  s += F("\"}}");

  return s;
}

// {"StatusSNS":{"Time":"...","Switch1":"ON"}}  <- é o que o Home Assistant consulta (STATUS 10)
String jsonStatusSNS()
{
  return String("{\"StatusSNS\":{") + corpoSwitch() + "}}";
}

String jsonStatusSTS()
{
  String s = F("{\"StatusSTS\":");
  s += jsonEstado();
  s += F("}");

  return s;
}

// STATUS (sem argumento), STATUS 0 (tudo), 2, 5, 8, 10 e 11. Responde em stat/<topico>/STATUS<n>.
String comandoStatus(const String &arg, bool viaMqtt)
{
  long n = ehNumero(arg) ? arg.toInt() : -1;
  String r;

  if (n == 0)
  {
    String a = jsonStatus();
    String b = jsonStatusFWR();
    String c = jsonStatusNET();
    String d = jsonStatusSNS();
    String e = jsonStatusSTS();

    if (viaMqtt)
    {
      mqttPub(tStat + "STATUS",   a, false);
      mqttPub(tStat + "STATUS2",  b, false);
      mqttPub(tStat + "STATUS5",  c, false);
      mqttPub(tStat + "STATUS10", d, false);
      mqttPub(tStat + "STATUS11", e, false);
    }

    r  = "{";
    r += semChaves(a); r += ",";
    r += semChaves(b); r += ",";
    r += semChaves(c); r += ",";
    r += semChaves(d); r += ",";
    r += semChaves(e);
    r += "}";

    return r;
  }

  const char *sufixo = "STATUS";

  if (n == 2)       { r = jsonStatusFWR(); sufixo = "STATUS2";  }
  else if (n == 5)  { r = jsonStatusNET(); sufixo = "STATUS5";  }
  else if (n == 8)  { r = jsonStatusSNS(); sufixo = "STATUS8";  }
  else if (n == 10) { r = jsonStatusSNS(); sufixo = "STATUS10"; }
  else if (n == 11) { r = jsonStatusSTS(); sufixo = "STATUS11"; }
  else              { r = jsonStatus(); }

  if (viaMqtt)
    mqttPub(tStat + sufixo, r, false);

  return r;
}

void agendaReinicio(unsigned long ms, bool resetaWifi)
{
  reiniciarPendente = true;
  resetarWifiPendente = resetaWifi;
  reiniciarEm = millis() + ms;
}

void finalizaMQTT()
{
  if (ConexaoMQTT.connected())
  {
    mqttPub(tTele + "LWT", "Offline", true);
    ConexaoMQTT.disconnect();
  }
}

// Executa um comando vindo do MQTT (viaMqtt = true, resposta também publicada em stat/)
// ou do HTTP /cm (viaMqtt = false, resposta só retornada). Devolve o JSON de resposta.
String executaComando(String cmd, String arg, bool viaMqtt)
{
  String resp;

  cmd.trim();
  cmd.toUpperCase();
  arg.trim();

  if (cmd == "STATE")
  {
    resp = jsonEstado();

    if (viaMqtt)
      mqttPub(tTele + "STATE", resp, false);

    return resp;
  }

  if (cmd == "STATUS")
    return comandoStatus(arg, viaMqtt);

  if (cmd == "RESTART")
  {
    if (arg == "1")
    {
      resp = F("{\"Restart\":\"Restarting\"}");
      agendaReinicio(1000UL, false);
    }
    else
      resp = F("{\"Restart\":\"1 to restart\"}");
  }
  else if (cmd == "TELEPERIOD")
  {
    // o aparelho trabalha em minutos inteiros (2 a 60); o valor em segundos é arredondado para cima
    if (ehNumero(arg))
    {
      long v = arg.toInt();

      if (v == 1)
        defineTempoMin(TEMPO_PADRAO_MIN);
      else if (v > 0)
      {
        long minutos = (v + 59) / 60;

        if (minutos < 2)  minutos = 2;
        if (minutos > 60) minutos = 60;

        defineTempoMin(minutos);
      }
    }

    resp = F("{\"TelePeriod\":");
    resp += (unsigned long)(TempoDeEspera / SEGUNDO);
    resp += F("}");
  }
  else
    resp = F("{\"Command\":\"Unknown\"}");

  if (viaMqtt)
    mqttPub(tStat + "RESULT", resp, false);

  return resp;
}

// =====================================================
// MQTT - CONEXÃO E RECEPÇÃO
// =====================================================

// Chamada de dentro do PubSubClient::loop(). Só guarda o comando; quem executa é o
// loop() principal (publicar aqui dentro sobrescreveria o buffer do payload recebido).
void recebendoMensagensMQTT(char* topic, byte* payload, unsigned int length)
{
  String t(topic);
  String arg;
  unsigned int n = (length > 64) ? 64 : length;

  arg.reserve(n);

  for (unsigned int i = 0; i < n; i++)
    arg += (char)payload[i];

  // o Home Assistant avisa que reiniciou: republica o discovery e o estado
  if (t == "homeassistant/status")
  {
    arg.trim();

    if (arg == "online")
      haOnlinePendente = true;

    return;
  }

  if (cmdPendente)
    return;

  cmdPend = t.substring(t.lastIndexOf('/') + 1);
  argPend = arg;
  cmdPendente = true;
}

void processaComandoPendente()
{
  if (!cmdPendente)
    return;

  String c = cmdPend;
  String a = argPend;

  cmdPendente = false;

  executaComando(c, a, true);
}

bool reconectaMQTT()
{
  if (ConexaoMQTT.connected())
    return true;

  if (dev.IPMqtt.length() == 0 || dev.Porta.toInt() == 0)
  {
    Serial.println("Falta configuração do servidor MQTT");
    return false;
  }

  String lwt = tTele + "LWT";
  bool ok;

  Serial.print("Conectando ao Broker MQTT: ");
  Serial.print(dev.IPMqtt); Serial.print(":"); Serial.print(dev.Porta); Serial.print("?");
  Serial.println(dev.Mqttuser);

  // LWT igual ao do Tasmota: se o aparelho cair, o broker publica "Offline"
  if (dev.Mqttuser.length())
    ok = ConexaoMQTT.connect(idCliente.c_str(), dev.Mqttuser.c_str(), dev.Mqttpass.c_str(), lwt.c_str(), 0, true, "Offline");
  else
    ok = ConexaoMQTT.connect(idCliente.c_str(), nullptr, nullptr, lwt.c_str(), 0, true, "Offline");

  if (!ok)
  {
    Serial.println("Falha na conexão MQTT");
    return false;
  }

  Serial.println("MQTT conectado");
  contadorMqtt++;

  ConexaoMQTT.subscribe((tCmnd + "+").c_str());
  ConexaoMQTT.subscribe((tGrupo + "+").c_str());
  ConexaoMQTT.subscribe("homeassistant/status");

  RegistraSensor(true);
  mqttPub(lwt, "Online", true);   // o HA responde ao "Online" pedindo cmnd/<topico>/STATUS 10
  publicaInfo();
  EnviaEstadoMQTT(true);
  publicaTele();

  lastChange = millis();
  WatchdogMQTT_Contador++;

  return true;
}

// Chamado a cada TempoDeEspera: re-registra o sensor e publica o estado (como o watchdog original)
void WatchdogMQTT()
{
  if (!Ativo) return;

  Serial.println("++++++++++++++++++++ WatchdogMQTT() ++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
  Serial.print("TempoDeEspera = ");
  Serial.println((unsigned long)(TempoDeEspera / MINUTO));

  lastChange = millis(); // update debounce time
  WatchdogMQTT_Contador++;

  RegistraSensor(false);
  EnviaEstadoMQTT(false);
  publicaTele();
}

// Conexão sem travar o aparelho: tenta a cada 5 s, dobrando até 60 s enquanto o broker estiver fora.
// (Antes, cada tentativa falha bloqueava o loop() por 2 s com delay(), atrasando a leitura do sensor.)
void gerenciaMQTT()
{
  if (!Ativo)
    return;

  if (ConexaoMQTT.connected())
  {
    ConexaoMQTT.loop();
    processaComandoPendente();

    if (!ConexaoMQTT.connected())
      return;

    if (haOnlinePendente)
    {
      haOnlinePendente = false;
      RegistraSensor(false);
      EnviaEstadoMQTT(true);
    }

    if ((millis() - lastChange) >= TempoDeEspera)   // Aguarda o tempo necessário antes de enviar sonda
      WatchdogMQTT();

    // mensagem de "estou no ar" (uma vez por boot), depois que o relógio NTP acertou
    if (mensagemBootPendente && (time(nullptr) > 1000000000L || millis() > 20000UL))
    {
      mensagemBootPendente = false;

      String msg = dev.MqttNomeAmigavel + ": " + WiFi.localIP().toString();
      sendMessage(msg.c_str());
    }

    return;
  }

  if (WiFi.status() != WL_CONNECTED)
    return;

  unsigned long agora = millis();

  if ((agora - ultimaTentativaMqtt) < intervaloMqtt)
    return;

  ultimaTentativaMqtt = agora;

  if (reconectaMQTT())
    intervaloMqtt = 0;
  else if (intervaloMqtt == 0)
    intervaloMqtt = 5000UL;
  else
  {
    intervaloMqtt *= 2;

    if (intervaloMqtt > 60000UL)
      intervaloMqtt = 60000UL;
  }
}

// =====================================================
// WEB
// =====================================================

// http://192.168.4.1/
void handleRoot()
{
  String msg, tag, valor;
  String index = "";

  msg = "<html><head><meta http-equiv='refresh' content='5'><title>MQTT-ENERGIA ELETRICA</title></head><body>";

  msg += "<br><br><br> *** ";
  msg += TEXTO_ABERTURA;
  msg += " v";
  msg += VERSAO;
  msg += " *** <br><br><pre>";
  msg += "HOST....: " + htmlEsc(dev.Host);
  msg += "<br>";
  msg += "IP......: " + WiFi.localIP().toString();
  valor = WiFi.SSID();
  msg += "<br>";
  msg += "SSID....: " + htmlEsc(valor);

  msg += "<br><br>SERVIDOR MQTT";
  msg += "<br>";
  msg += "IP/User.: " + htmlEsc(dev.IPMqtt) + ":" + htmlEsc(dev.Porta) + "?" + htmlEsc(dev.Mqttuser);
  msg += "<br>";
  msg += "Conexao.: ";
  msg += ConexaoMQTT.connected() ? "conectado" : "desconectado";
  msg += "<br>";
  msg += "Nome....: " + htmlEsc(dev.MqttNomeAmigavel);
  msg += "<br>";
  msg += "Topico..: " + htmlEsc(dev.MqttTopico);
  msg += "<br>";
  valor = textoEstado();
  msg += "Estado..: " + valor;

  msg += "<br>";
  msg += "<br>";

  String x = readFile(ARQ_CNF_TEMPO);
  msg += "debug ARQ_CNF_TEMPO...: " + htmlEsc(x);
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
      msg += " restart<br>";
      msg += " status<br>";
      msg += " status/json<br>";
      msg += " cm?cmnd=Status 10<br>";
      msg += " update<br>";
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
  agendaReinicio(500UL, false);
}

// apaga as credenciais do Wi-Fi e reinicia (volta ao portal de configuração)
void handleRestart() {
  server.send(200, "text/plain", "+OK RESTART");
  agendaReinicio(500UL, true);
}

void handleStatusJson() {
  String json = "{\"estado\":\"";

  json += textoEstado();
  json += "\",\"DataHora\":\"";
  json += textoDataHoraBR();
  json += "\",\"mqtt\":";
  json += ConexaoMQTT.connected() ? "true" : "false";
  json += "}";

  server.send(200, "application/json", json);
}

void handleStatus() {
  server.send(200, "text/plain", textoEstado());
}

// http://192.168.4.1/config?Host=HostMqttSP&IP=192.168.3.7&Porta=1883&MqttUser=mqtt&MqttPass=<senha>&MqttNomeAmigavel=Interruptor Geral Ibiuna&MqttTopico=InterruptorGeralIbiuna
void handleConfig()
{
  String msg, tag, valor, Host, IP, Porta, Mqttuser, Mqttpass, MqttNomeAmigavel, MqttTopico;

  msg = "";

  // (antes o loop parava em MQTTTOPICO, ignorando os parâmetros que viessem depois dele na URL)
  for (uint8_t i=0; i<server.args(); i++){
    tag   = server.argName(i);
    valor = server.arg(i);
    tag.toUpperCase();

    if (tag == "HOST")
    {
      Host = valor;
      msg += " Host:" + Host;
    }
    else if (tag == "IP")
    {
      IP = valor;
      msg += " IP:" + IP;
    }
    else if (tag == "PORTA")
    {
      Porta = valor;
      msg += " Porta:" + Porta;
    }
    else if (tag == "MQTTUSER")
    {
      Mqttuser = valor;
      msg += " Mqttuser:" + Mqttuser;
    }
    else if (tag == "MQTTPASS")
    {
      Mqttpass = valor;
      msg += " Mqttpass:****";   // a senha não é mais devolvida na resposta
    }
    else if (tag == "MQTTNOMEAMIGAVEL")
    {
      MqttNomeAmigavel = valor;
      msg += " MqttNomeAmigavel:" + MqttNomeAmigavel;
    }
    else if (tag == "MQTTTOPICO")
    {
      MqttTopico = sanitizaTopico(valor);
      msg += " MqttTopico:" + MqttTopico;
    }
  }

  if (Host.length() != 0 && IP.length() != 0 && Porta.length() != 0 && Mqttuser.length() != 0 && Mqttpass.length() != 0&& MqttNomeAmigavel.length() != 0 && MqttTopico.length() != 0)
  {
    Serial.println(Host);
    Serial.println(IP);
    Serial.println(Porta);
    Serial.println(Mqttuser);
    Serial.println(MqttNomeAmigavel);
    Serial.println(MqttTopico);

    gravaConfigEx(Host, IP, Porta, Mqttuser, Mqttpass, MqttNomeAmigavel, MqttTopico);

    server.send(200, "text/plain", "+OK Config-X" + msg);

    Serial.println(" ");
    Serial.println("reboot");

    agendaReinicio(1000UL, false);
  }
  else
    server.send(200, "text/plain", "-ERR Config-X");
}

// http://192.168.4.1/configTempo?tempo=5   (minutos entre as publicações periódicas, 2 a 60)
void handleConfigTempo()
{
  String t = server.hasArg("tempo") ? server.arg("tempo") : String("");

  t.trim();

  if (ehNumero(t) && defineTempoMin(t.toInt()))
    server.send(200, "text/plain", "+OK configTempo " + t);
  else
    server.send(200, "text/plain", "-ERR configTempo (use tempo=2 a 60)");
}

// API de comandos do Tasmota: /cm?cmnd=Status%2010  ->  {"StatusSNS":{...}}
void handleCm()
{
  String c = server.hasArg("cmnd") ? server.arg("cmnd") : String("");

  c.trim();

  if (!c.length())
  {
    server.send(200, "application/json", "{\"Command\":\"Unknown\"}");
    return;
  }

  String cmd = c;
  String arg = "";
  int sp = c.indexOf(' ');

  if (sp >= 0)
  {
    cmd = c.substring(0, sp);
    arg = c.substring(sp + 1);
  }

  server.send(200, "application/json", executaComando(cmd, arg, false));
}

// =====================================================
// LEITURA DO SENSOR
// =====================================================

// Lê o pino a cada LEITURA_MS (antes era delay(300) no loop, que também atrasava a página web).
void amostrarEntrada()
{
  static unsigned long ultimaAmostra = 0;
  unsigned long agora = millis();

  if (agora - ultimaAmostra < LEITURA_MS)
    return;

  ultimaAmostra = agora;

  int LeituraSwitchD1Pin = digitalRead(PIN_BOTAO);

  if (LeituraSwitchD1Pin != UltimoValor)
  {
    UltimoValor = LeituraSwitchD1Pin;
    EnviaEstadoMQTT(true);
  }

  if (TimeCounter < ESTADO_PERIODICO_LEITURAS)
    TimeCounter++;
  else
  {
    Serial.println("TimeCounter automatico");
    UltimoValor = LeituraSwitchD1Pin;
    EnviaEstadoMQTT(false);
    TimeCounter = 0;
  }
}

//////////////////////////////////////////////////////////////  SETUP
void setup()
{
  // só TX: o pino RX (GPIO3) é a entrada do sensor
  Serial.begin(BAUD_RATE, SERIAL_8N1, SERIAL_TX_ONLY);

  //pinMode(PIN_BOTAO, INPUT_PULLUP);
  pinMode(PIN_BOTAO, INPUT);
  pinMode(PIN_LED, OUTPUT);

  UltimoValor = digitalRead(PIN_BOTAO);
  atualizaLed();

  openFS();

  // SPIFFS.format();

  carregaConfigEx();
  setupWiFiAuto();

  if (dev.Host.length() == 0)
    dev.Host = HOST_NOME;

  montaTopicos();
  carregaTempo();

  // https://dl.dropboxusercontent.com/s/4e2puj5noup64jt/testeURL.txt
  server.on("/", handleRoot);
  //Envia o arquivo html quando entrar quando fizerem uma requisição na raiz do servidor
  //server.serveStatic("/", SPIFFS, "/automacao.html");
  server.on("/reboot", handleReboot);
  server.on("/config", handleConfig);
  server.on("/configTempo", handleConfigTempo);

  server.on("/status", handleStatus);
  server.on("/status/json", handleStatusJson);
  server.on("/restart", handleRestart);
  server.on("/cm", handleCm);


  //listSSIDex();
  //while(1) server.handleClient();

  MDNS.begin(dev.Host);

  // Inicia o servidor WEB
  atualizadorOTA.setup(&server);
  server.begin();

  MDNS.addService("http", "tcp", 80);

  Serial.println("Server iniciado");

  Serial.printf("HTTPUpdateServer pronto! Use: http://%s.local/update no seu browser\n", dev.Host.c_str());


  Serial.print("\r\n"); Serial.print(TEXTO_ABERTURA); Serial.print(" - v"); Serial.print(VERSAO); Serial.println("\r\n\r\n");

  Serial.print("SSID............: "); Serial.println(WiFi.SSID());
  Serial.print("Endereço de IP..: "); Serial.println(WiFi.localIP());
  Serial.print("MAC.............: "); Serial.println(WiFi.macAddress());

  //wfm.resetSettings();
  //ESP.restart();

  ConexaoMQTT.setServer(dev.IPMqtt.c_str(), dev.Porta.toInt());
  ConexaoMQTT.setCallback(recebendoMensagensMQTT);
  ConexaoMQTT.setBufferSize(MQTT_BUFFER);
  ConexaoMQTT.setKeepAlive(30);
  ConexaoMQTT.setSocketTimeout(3);
  wifiClient.setTimeout(2000);

  configTime(UTC_OFFSET, UTC_OFFSET_DST, NTP_SERVER);

  Serial.println("\r\nOK\r\n\r\n");

  gerenciaMQTT();   // primeira tentativa; se falhar, o loop() tenta de novo sem travar
}

//////////////////////////////////////////////////////////////  LOOP
void loop()
{
  uptimeMs();   // mantém o contador de uptime sem estouro

  server.handleClient();
  MDNS.update();

  gerenciaMQTT();   // reconexão, comandos recebidos, discovery e publicação periódica

  amostrarEntrada();

  if (reiniciarPendente && (long)(millis() - reiniciarEm) >= 0)
  {
    finalizaMQTT();

    if (resetarWifiPendente)
      wfm.resetSettings();

    ESP.restart();
  }

  yield();
}

//
//  Tópicos Tasmota (substitua os valores entre < >)
//
//  mosquitto_pub -h <BROKER> -p 1883 -u <USUARIO> -P <SENHA> -t "cmnd/<TOPICO>/STATUS" -m "10"
//  mosquitto_pub -h <BROKER> -p 1883 -u <USUARIO> -P <SENHA> -t "cmnd/<TOPICO>/STATE"  -m ""
//  mosquitto_pub -h <BROKER> -p 1883 -u <USUARIO> -P <SENHA> -t "cmnd/<TOPICO>/TELEPERIOD" -m "300"
//
//  mosquitto_sub -h <BROKER> -p 1883 -u <USUARIO> -P <SENHA> -t "+/<TOPICO>/#" -v
//
//  Tópico antigo (continua publicando com MQTT_LEGADO 1)
//
//  mosquitto_sub -h <BROKER> -p 1883 -u <USUARIO> -P <SENHA> -t "esp8266/<TOPICO>/state" -v
//
//  HTTP
//
//  curl "http://<IP>/cm?cmnd=Status%2010"
//  curl "http://<IP>/status"
//  curl "http://<IP>/configTempo?tempo=5"
//
