import os,time
from os import path

from confluent_kafka import Producer
p = Producer({'bootstrap.servers': 'localhost:9092',
              'message.max.bytes': 15728640})
#fileName='audiofiles/darija1.flac'
directory=path.join(path.dirname(path.realpath(__file__)),"audiofiles")
listFilesProcessed=[]

logsFile=path.join(path.dirname(path.realpath(__file__)),"audiofiles/logs_FilesProcessed.txt")
#print(logsFile)
def getByteFromMyAudio(fileName):
    return open(fileName, "rb").read()
def ReadLogs():
    try:

        with open(logsFile, 'r') as f:
            print("Reading Logs File,List Files Processed from Logs File: " + logsFile)
            print(
                '--------------------------------------------------------------------------------------------------------')

            for line in f:

                audiofile = line[:-1]
                listFilesProcessed.append(audiofile)
            print("Number of files processed from our Log File: {}".format(len(listFilesProcessed)))
    except FileNotFoundError:
        print("No logs File, 1st Launch of the program , All audio files of the directory will be processed")
        print('--------------------------------------------------------------------------------------------------------')




def SaveLogs():
    with open(logsFile, 'w') as f:
        for item in listFilesProcessed:
            f.write("%s\n" % item)
        f.close()
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Audio File delivery failed: {}'.format(err))
    else:
        print('Audio File delivered to Topic: {}, Partition: {}, Offset {}'.format(msg.topic(), msg.partition(),msg.offset()))
        print('--------------------------------------------------------------------------------------------------------')

p.poll(0)
ReadLogs()
#print ("List Files Processed from Logs File: {}".format(listFilesProcessed))
try:
    while True:
        for filename in os.listdir(directory):
            if (filename.endswith(".flac") or filename.endswith(".wav")) and (filename not in listFilesProcessed):
                listFilesProcessed.append(filename)
                path=os.path.join(directory, filename)
                time.sleep(2)
                bt=getByteFromMyAudio(path)
                time.sleep(2)
                print("Producing Audio File : {} to Kafka".format(os.path.join(directory, filename)))
                p.produce('audiosource', key=filename, value=bt, callback=delivery_report)
                p.flush()
                continue
            else:
                continue



except KeyboardInterrupt:
    SaveLogs()
    print("Logs saved to {} ".format(logsFile))
    print('interrupted!')

