import paho.mqtt.client as mqtt
from datetime import datetime
import subprocess

Topics = [["IUT/Colmar2024/SAE2.04/Maison1",1], ["IUT/Colmar2024/SAE2.04/Maison2",2]]
database_ip = "bdd.rt4.lab"
database_user = "root"
database_password = "bddgrp4"
db_name = "sensors"

class Message:
    def __init__(self, Id, piece, date, time, temp):
        self.Id = Id
        self.piece = piece
        self.date = date
        self.time = time
        self.temp = float(temp)
        # on doit mettre date et time ensempble pour par la suite aller dans un champ datetime de la base de données mysql
        # il faut bien faire attention au format de la date et du temps pour que cela soit compatible avec mysql
        # on doit donc convertir le format de la date et du temps pour qu'il soit compatible avec mysql
        self.datetime = self.get_mysql_datetime()

    def get_mysql_datetime(self):
        datetime_obj = datetime.strptime(self.date + " " + self.time, "%d/%m/%Y %H:%M:%S")
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")



class MQTTClient:
    def __init__(self, client_id, broker, port):
        self.messageBuffer = []
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(broker, port)
        print("Connected to broker, starting loop...")
        while True:
            self.client.loop()
            if len(self.messageBuffer) > 0:
                msg = self.messageBuffer[0]  # Just peek at the first message, don't remove it yet
                print("Message received : ", msg.__dict__)
                if self.send_to_database(msg):
                    self.messageBuffer.pop(0)  # Only remove the message if it was sent successfully
        print("Loop ended")

    def send_to_database(self, message):
        # Check if sensor exists in maison_capteur table
        cmd = f"mysql -u {database_user} -p{database_password} -h {database_ip} -e \"SELECT id FROM {db_name}.maison_capteur WHERE id='{message.Id}'\""
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout.strip() == '':
            # Sensor does not exist, insert into maison_capteur table
            cmd = f"mysql -u {database_user} -p{database_password} -h {database_ip} -e \"INSERT INTO {db_name}.maison_capteur (id, nomCapteur, Emplacement) VALUES ('{message.Id}', 'your_sensor_name', '{message.piece}')\""
            result = subprocess.run(cmd, shell=True)
            if result.returncode != 0:
                return False  # Return False if the command failed
        # Insert into maison_donnee table
        cmd = f"mysql -u {database_user} -p{database_password} -h {database_ip} -e \"INSERT INTO {db_name}.maison_donnee (temperature, timestamp, id_capteur_id) VALUES ('{message.temp}', '{message.datetime}', '{message.Id}')\""
        result = subprocess.run(cmd, shell=True)
        return result.returncode == 0

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        for topic in Topics:
            client.subscribe(topic[0])

    def parse_message(self, msg):
        split = str(msg.payload).split(",")
        Id = split[0].split("=")[1]
        piece = msg.topic.split("/")[-1] + "-" + split[1].split("=")[1]
        date = split[2].split("=")[1]
        time = split[3].split("=")[1]
        temp = split[4].split("=")[1][:-1]
        self.messageBuffer.append(Message(Id, piece, date, time, temp))

    def on_message(self, client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))
        # on recoi des messages type : Id=A72E3F6B79BB,piece=chambre1,date=19/06/2024,time=13:50:10,temp=0.22
        try :
            self.parse_message(msg)
        except Exception as e:
            print("Error parsing message : ", e)
            print("Message : ", msg.payload)
            print("Maybe its not the right format ?")


if __name__ == "__main__":
    client = MQTTClient("client1", "test.mosquitto.org", 1883)
