import serial
import time
from binascii import unhexlify
import sys
import string
import paho.mqtt.client as mqtt
from gurux_dlms.GXDLMSTranslator import GXDLMSTranslator
from gurux_dlms.GXDLMSTranslatorMessage import GXDLMSTranslatorMessage
from gurux_dlms.GXByteBuffer import GXByteBuffer
from gurux_dlms.TranslatorOutputType import TranslatorOutputType
from bs4 import BeautifulSoup
from xml.etree.ElementTree import XML, fromstring
from time import sleep

# EVN Schlüssel eingeben zB. "36C66639E48A8CA4D6BC8B282A793BBB"
evn_schluessel = "EVN Schlüssel"

#MQTT Verwenden (True | False)
useMQTT = False

#MQTT Broker IP adresse Eingeben ohne Port!
mqttSSL = False
mqttBroker = "127.0.0.1"
mqttuser =""
mqttpasswort = ""
mqttport = 1883
mqttTopic = "Smartmeter"

#Comport Config/Init
comport = "/dev/ttyUSB0"

#Aktulle Werte auf Console ausgeben (True | False)
printValue = True

# Holt Daten von serieller Schnittstelle
def recv(serialIncoming):
    while True:
        data = serialIncoming.read_all()
        if data == '':
            continue
        else:
            break
        sleep(0.5)
    return data

#MQTT Init
if useMQTT:
    try:
        client = mqtt.Client("SmartMeter_"+str(uuid.uuid1()))
        client.username_pw_set(mqttuser, mqttpasswort)
        if mqttSSL:
            client.tls_set(certifi.where())        
        client.connect(mqttBroker, mqttport)
        client.loop_start()
    except:
        print("Die Ip Adresse des Brokers ist falsch!")
        sys.exit()
    
tr = GXDLMSTranslator(TranslatorOutputType.SIMPLE_XML)
tr.blockCipherKey = GXByteBuffer(evn_schluessel)
tr.comments = True

serIn = serial.Serial( port=comport,
         baudrate=2400,
         bytesize=serial.EIGHTBITS,
         parity=serial.PARITY_NONE,
         stopbits=serial.STOPBITS_ONE
)

stream = ""
daten = ""

while 1:
    sleep(.25)
    stream += recv(serIn).hex()
    spos = stream.find("68fafa68")
    if spos != -1:
        stream = stream[spos:]
        if len(stream) < 564 : continue
        daten = stream[:564]
        stream = stream[564:]
    else:
        if len(stream) > (564 * 10) :
            print ("Missing Start Bytes... waiting")
            stream = ""
        continue

    try:
        msg = GXDLMSTranslatorMessage()
        msg.message = GXByteBuffer(daten)
        xml = ""
        pdu = GXByteBuffer()
        tr.completePdu = True
        while tr.findNextFrame(msg, pdu):
            pdu.clear()
            xml += tr.messageToXml(msg)

        frameCounter = int(daten[44:52],16)
        soup = BeautifulSoup(xml, 'lxml')
        results_32 = soup.find_all('uint32')
        results_16 = soup.find_all('uint16')

    except BaseException as err:
        print("Fehler: ", format(err))
        continue
       
    try:
        
        WirkenergieP = int(str(results_32)[16:16+8],16)             #Wirkenergie A+ in Wattstunden        
        WirkenergieN = int(str(results_32)[52:52+8],16)             #Wirkenergie A- in Wattstunden        
        MomentanleistungP = int(str(results_32)[88:88+8],16)        #Momentanleistung P+ in Watt
        MomentanleistungN = int(str(results_32)[124:124+8],16)      #Momentanleistung P- in Watt        
        SpannungL1 = int(str(results_16)[16:20],16)/10              #Spannung L1 in Volt        
        SpannungL2 = int(str(results_16)[48:52],16)/10              #Spannung L2 in Volt        
        SpannungL3 = int(str(results_16)[80:84],16)/10              #Spannung L3 in Volt        
        StromL1 = int(str(results_16)[112:116],16)/100              #Strom L1 in Ampere    
        StromL2 = int(str(results_16)[144:148],16)/100              #Strom L2 in Ampere        
        StromL3 = int(str(results_16)[176:180],16)/100              #Strom L3 in Ampere        
        Leistungsfaktor = int(str(results_16)[208:212],16)/1000     #Leistungsfaktor
                        
        if printValue:
            print("\n\t\t*** Daten vom Smartmeter (" + str(frameCounter) + ") ***\n\nBezeichnung\t\t Wert")
            print('Wirkenergie+\t\t ' + str(WirkenergieP) + ' Wh')
            print('Wirkenergie-\t\t ' + str(WirkenergieN) + ' Wh')
            print('Momentanleistung+\t ' + str(MomentanleistungP) + ' W')
            print('Momentanleistung-\t ' + str(MomentanleistungN) + ' W')
            print('Spannung L1\t\t ' + str(SpannungL1) + ' V')
            print('Spannung L2\t\t ' + str(SpannungL2) + ' V')
            print('Spannung L3\t\t ' + str(SpannungL3) + ' V')
            print('Strom L1\t\t ' + str(StromL1) + ' A')
            print('Strom L2\t\t ' + str(StromL2) + ' A')
            print('Strom L3\t\t ' + str(StromL3) + ' A')
            print('Leistungsfaktor\t\t ' + str(Leistungsfaktor))
            print('Momentanleistung\t ' + str(MomentanleistungP-MomentanleistungN) + ' W')
            print('')
        
        #MQTT
        if useMQTT:
            client.publish(mqttTopic + "/Frame",frameCounter)
            client.publish(mqttTopic + "/WirkenergieP",WirkenergieP)
            client.publish(mqttTopic + "/WirkenergieN",WirkenergieN)
            client.publish(mqttTopic + "/MomentanleistungP",MomentanleistungP)
            client.publish(mqttTopic + "/MomentanleistungN",MomentanleistungN)
            client.publish(mqttTopic + "/Momentanleistung",MomentanleistungP - MomentanleistungN)
            client.publish(mqttTopic + "/SpannungL1",SpannungL1)
            client.publish(mqttTopic + "/SpannungL2",SpannungL2)
            client.publish(mqttTopic + "/SpannungL3",SpannungL3)
            client.publish(mqttTopic + "/StromL1",StromL1)
            client.publish(mqttTopic + "/StromL2",StromL2)
            client.publish(mqttTopic + "/StromL3",StromL3)
            if client.publish(mqttTopic + "/Leistungsfaktor",Leistungsfaktor)[0] != 0 :
                print("Publish fehlgeschlagen!")
                client.connect(mqttBroker, mqttport)

    except BaseException as err:
        print("Fehler: ", format(err))
        continue
