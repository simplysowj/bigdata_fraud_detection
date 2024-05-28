from flask import Flask
from flask import request
from kafka import KafkaProducer
import os
import json

app = Flask(__name__)

@app.route("/add", methods = ['POST'])
def add_contact() :
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_data = request.json
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        producer.send('my-topic', json.dumps(json_data).encode('utf-8'))
        return json_data
    else:
        return 'Content-Type not supported!'
    
if __name__ == '__main__':
    app.run(debug = True, host = '0.0.0.0')
