from adisconfig import adisconfig
from adislog import adislog

from flask import Flask, request, redirect
from pymongo import MongoClient
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from uuid import uuid4
from time import time

application=Flask(__name__)
config=adisconfig('/etc/adistools/url_shortener.yaml')
log=adislog(
    backends=['terminal'],
    debug=True,
    replace_except_hook=False,
)
mongo_cli=MongoClient(
    config.mongo.host,
    config.mongo.port
)
mongo_db=mongo_cli[config.mongo.db]
urls=mongo_db['shortened_urls']
metrics=mongo_db['shortened_urls_metrics']
rabbitmq_connection=None

def open_rabbitmq_conn():
    rabbitmq_connection=BlockingConnection(
        ConnectionParameters(
            host=config.rabbitmq.host,
            port=config.rabbitmq.port,
            credentials=PlainCredentials(
                config.rabbitmq.user,
                config.rabbitmq.passwd
                )
            )
        )

def close_rabbitmq_conn():
    rabbitmq_connection.close()
    
def send_to_queue(message):
    open_rabbitmq_conn()
    channel=rabbitmq_connection.channel()
    channel.basic_publish(exchange="",
                            routing_key="url_shortener", 
                            body=message, 
                            )
    close_rabbitmq_conn()
        

@application.route("/<redirection_query>", methods=['GET'])
def redirect(redirection_query):
    query={
        'redirection_query' : redirection_query
    }
    data=urls.find_one(query)

    if data:
        url_uuid=data['url_uuid']
        redirection_uuid=str(uuid4())
        user_agent=str(request.user_agent)
        if request.headers.getlist("X-Forwarded-For"):
            ip_addr= request.headers.getlist("X-Forwarded-For")[0]
        else:
            ip_addr=str(request.remote_addr)
        timestamp=time()

        document={
            "url_uuid"          : url_uuid,
            "redirection_uuid"  : redirection_uuid,
            "redirection_query" : redirection_query,
            "ip_addr"           : ip_addr,
            "user_agent"        : user_agent,
            "timestamp"         : timestamp,
            "host_details"      : None        
            }

        metrics.insert_one(document)


        return Flask.redirect(
            application,
            location=data['url'],
            code=302
        )
    else:
        return ""
