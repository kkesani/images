# from pymongo.bi
# from bson.binary import Binary
from pymongo import MongoClient
from PIL import Image
import base64
import time
import glob, os
import sys, json
from publisher_subscriber import DataPublisher

publisher = DataPublisher()
from detectobject import detect_object

def mongo_connection_with_collection():
    client = MongoClient('mongodb://localhost:27017')
    db = client['kkesani_3']
    col = db.image
    col1 = db.config
    return col, col1


def fetch_image(image_str, count):
    fh = open("/home/EOG/kkesani/fetched_images/{}.jpg".format(count), "wb")
    fh.write(base64.b64decode(image_str))
    fh.close()

def image_to_str(img):
    with open(img, "rb") as imageFile:
        img_str = base64.b64encode(imageFile.read())
    return img_str

while True:
    #data_path = "C:\\Users\\kkesani\\Desktop\\images"
    data_path = "/home/eogftp"
    files_with_path = glob.glob(data_path + "/*")
    print("Got {} files on  {}".format(len(files_with_path),data_path))
    col, col1 = mongo_connection_with_collection()
    IMAGE_SYNC = os.getenv("IMAGE_SYNC")
    if IMAGE_SYNC is None:
        IMAGE_SYNC = True
    else:
        if IMAGE_SYNC.lower() == 'true':
            IMAGE_SYNC = True
        elif IMAGE_SYNC.lower() == 'false':
            IMAGE_SYNC = False
    PRIMO_ID = 12333234
    IP = "123.123.123.123"
    prediction_image="/home/EOG/kkesani/darknet/predictions.jpg"
    for img in files_with_path:
        print("Executing for {}".format(img))
        img_str = image_to_str(img)
        predictions_image = image_to_str(prediction_image)
        objects = detect_object(img)
        #objects = {}
        print("Got objects")
        data_dict = {
            'objects': json.dumps(objects),
            'timestamp': int(time.time()),
            'image': img_str,
            'predictions_image': predictions_image,
            'sync': IMAGE_SYNC,
            'altered': "Yes"
        }
        col.insert_one(data_dict)
        if IMAGE_SYNC:
            data_dict['predictions_image'] = predictions_image.decode()
            #print(type(data_dict['image']))
            del data_dict['_id']
            del data_dict['sync']
            del data_dict['altered']
            del data_dict['image']
            data_dict['PRIMO_ID'] = PRIMO_ID
            data_dict['IP'] = IP
            print("Sending image to pubsub")
            publisher.send(json.dumps(data_dict))
            time.sleep(1)
        col1.insert_one(
            {
                'PRIMO_ID': 12345,
                'NAME': "ConfigName",
                'IP': IP,
                'SYNC': IMAGE_SYNC,
                'ALERT': "NO"
            }
        )
        print("{} inserted to db".format(img))
        os.remove(img)
    # wait for 1 minute
    time.sleep(5)

'''
client = MongoClient('mongodb://localhost:27017')
db = client['kkesani_2']

for count, img in enumerate(db.image.find()):
    img_str = img['image']
# image = fordb.image.find()[3]['image']
    fetch_image(img_str, count)
#

'''
