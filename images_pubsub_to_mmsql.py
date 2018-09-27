from publisher_subscriber import DataConsumer
import pymysql
import json, time
import base64
import datetime, os


def fetch_image(image_str, PRIMO_ID):
    # fh = open(/home/EOG/kkesani/fetched_images/{}.jpg".format(count), "wb")
    name = str(datetime.datetime.now()).replace(' ', '-').replace(":","_")
    # filepath = "C:\\Users\\kkesani\\Desktop\\output\\{}".format(PRIMO_ID)
    filepath = "/home/EOG/kkesani/fetched_images/{}".format(PRIMO_ID)
    if not os.path.exists(filepath):
        os.mkdir(filepath)
    complete_path = "{}/{}.jpg".format(filepath, name)
    # fh = open("{}\\{}.jpg".format(filepath, name), "wb")
    fh = open(complete_path, "wb")
    fh.write(base64.b64decode(image_str))
    fh.close()
    print(complete_path)
    return complete_path

class PubSubToDataBase(object):
    def __init__(self, host, user, password, db):
        self.conn = pymysql.connect(host=host, user=user, passwd=password, db=db)
        self.cur = self.conn.cursor()
        self.LC = DataConsumer()

    def execute(self, console_display = True):
        print("waiting for messages...")
        while True:
            print(self.LC.queue.qsize())
            while self.LC.queue.qsize()>0:
                message = self.LC.get_messages()
                payload_dict = json.loads(message.decode('utf-8'))
                payload = json.loads(payload_dict.get('payload'))
                objects = json.loads(payload.get('objects'))
                if len(objects) > 0:
                    for k,v in objects.items():
                        score = v.split("%")[0]
                        image = payload.get('predictions_image')
                        timestamp = payload.get('timestamp')
                        PRIMO_ID = payload.get('PRIMO_ID')
                        hostname = payload.get('IP')
                        filepath = fetch_image(image,PRIMO_ID)
                        sql = 'Insert into image(image_object,score, filepath, hostname, timestamp, created_at)values(%s, %s, %s, %s, %s, now())'
                        val = (k,score, filepath, hostname, timestamp)
                        self.cur.execute(sql, val)
                        self.conn.commit()
                else:
                    image = payload.get('image')
                    timestamp = payload.get('timestamp')
                    PRIMO_ID = payload.get('PRIMO_ID')
                    hostname = payload.get('IP')
                    filepath = fetch_image(image, PRIMO_ID)
                    sql = 'Insert into image(filepath, hostname, timestamp, created_at)values(%s, %s, %s, now())'
                    val = (filepath, hostname, timestamp)
                    self.cur.execute(sql, val)
                    self.conn.commit()
            time.sleep(5)
if __name__ == "__main__":
    PubSubToDataBase(host='ktyvlxmsqlio01', user='root', password='hiVcgidXgiy2VABA3Ptu', db='camera_images').execute()
