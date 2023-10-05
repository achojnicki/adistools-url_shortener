from adisconfig import adisconfig
from adislog import adislog

from flask import Flask, request, redirect
from pymongo import MongoClient
from uuid import uuid4
from time import time

application=Flask(__name__)
config=adisconfig('/opt/adistools/configs/url_shortener.yaml')
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
            }

        metrics.insert_one(document)


        return Flask.redirect(
            application,
            location=data['url'],
            code=302
        )
    else:
        return ""
