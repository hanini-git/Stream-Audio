from confluent_kafka import Consumer, KafkaError,Producer
import speech_recognition as sr
import os
from os import path
import time
from threading import Thread



AudioSavePathFromConsumer=path.join(path.dirname(path.realpath(__file__)),"SavedAudioFromConsumers/")
TxTSavePathFromSpeech2Text=path.join(path.dirname(path.realpath(__file__)),"TxtResultsSpeech2Text/")

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['audiosource'])

p = Producer({'bootstrap.servers': 'localhost:9092'})

def SaveMyAudio(name,value):
    f = open(AudioSavePathFromConsumer+name, 'wb')
    f.write(value)
    f.close()
    print("Audio Decoded and Saved to "+AudioSavePathFromConsumer+name)

def delivery_report(err, msg):

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Text Results delivered back  to Kafka , Topic: {} ,Partition : {}, Offset: {}'.format(msg.topic(), msg.partition(),msg.offset()))
        print('--------------------------------------------------------------------------------------------------------')


def ProduceResultsToKafka(file,key):
    myfile = open(file, 'r')
    content=myfile.read()
    #print(content)
    p.poll(0)
    p.produce('textresults', key=key, value=content.encode("utf-8"), callback=delivery_report)
    p.flush()

def PlaySound(*args):
    print("Playing Audio File Started")
    os.system("vlc -Idummy '" +args[0]+ "' vlc://quit")


def SpeechToText(*args):
    AUDIO_FILE=args[0]
    key=args[1]
    r = sr.Recognizer()
    with sr.AudioFile(AUDIO_FILE) as source:
        audio = r.record(source)  # read the entire audio file

    try:
        txt = r.recognize_google(audio, language="ar-FR")
        print("Speech Recognition thinks you said: " + txt)
        f = open(TxTSavePathFromSpeech2Text+key+"_Results.txt", 'w+b')
        f.write(txt.encode('utf-8'))
        f.close()
        print("Saving Results txt File : "+TxTSavePathFromSpeech2Text+key+"_Results.txt ,\n Producing to Kafka")
        ProduceResultsToKafka(TxTSavePathFromSpeech2Text+key+"_Results.txt",key)

    except sr.UnknownValueError:
        print("Google Speech Recognition could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from Google Speech Recognition service; {0}".format(e))




while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    key=msg.key().decode("utf-8")
    print('--------------------------------------------------------------------------------------------------------')

    print('Received message: Audio From KAFKA : {}'.format(key,msg))
    SaveMyAudio(key,msg.value())
    print("Sending Audio File to Speech2Text")
    time.sleep(2)
    AUDIO_FILE=AudioSavePathFromConsumer + key
    ThreadSpeechToTextToKafka=Thread(target=SpeechToText,args=(AUDIO_FILE,key))
    ThreadPlayingAudio=Thread(target=PlaySound,args=[AUDIO_FILE])
    #SpeechToText(AudioSavePathFromConsumer+key,key)
    ThreadSpeechToTextToKafka.start()
    ThreadPlayingAudio.start()
    ThreadSpeechToTextToKafka.join()
    ThreadPlayingAudio.join()





c.close()