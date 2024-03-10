from adisconfig import adisconfig
from log import Log

from flask import Flask, request, redirect, render_template
from pymongo import MongoClient
from datetime import datetime
from html import escape

class URL_Shortener:
    project_name='adistools-url_shortener'
    def __init__(self):
        self._config=adisconfig('/opt/adistools/configs/adistools-url_shortener.yaml')
        self._log=Log(
            parent=self,
            backends=['rabbitmq_emitter'],
            debug=self._config.log.debug,
            rabbitmq_host=self._config.rabbitmq.host,
            rabbitmq_port=self._config.rabbitmq.port,
            rabbitmq_user=self._config.rabbitmq.user,
            rabbitmq_passwd=self._config.rabbitmq.password,
        )  

        self._mongo_cli=MongoClient(
            self._config.mongo.host,
            self._config.mongo.port,
        )

        self._mongo_db=self._mongo_cli[self._config.mongo.db]
        self._urls=self._mongo_db['shortened_urls']
        self._metrics=self._mongo_db['shortened_urls_metrics']

    def add_metric(self,redirection_uuid, redirection_query, remote_addr, user_agent, time):
        document={
            "redirection_uuid"  : redirection_uuid,
            "redirection_query" : redirection_query,
            "time"              : {

                "timestamp"         : time.timestamp(),
                "strtime"          : time.strftime("%m/%d/%Y, %H:%M:%S")
                },
            "client_details"    : {
                "remote_addr"       : remote_addr,
                "user_agent"        : user_agent,
                }
            }

        self._metrics.insert_one(document)
    def get_short_url(self, redirection_query):
        query={
            'redirection_query' : redirection_query
        }
        return self._urls.find_one(query)

url_shortener=URL_Shortener()
application=Flask(
    __name__,
    template_folder="template",
    static_folder='static'
    )

@application.route("/<redirection_query>", methods=['GET'])
def redirect(redirection_query):
    
    data=url_shortener.get_short_url(redirection_query)
    if data:
        time=datetime.now()
        redirection_uuid=data['redirection_uuid']
        user_agent=str(request.user_agent)
        if request.headers.getlist("X-Forwarded-For"):
            remote_addr=request.headers.getlist("X-Forwarded-For")[0]
        else:
            remote_addr=str(request.remote_addr)
        
        url_shortener.add_metric(
            redirection_query=redirection_query,
            redirection_uuid=redirection_uuid,
            remote_addr=remote_addr,
            user_agent=user_agent,
            time=time
            )

        return Flask.redirect(
            application,
            location=data['redirection_url'],
            code=302
        )
    else:
        return render_template(
            'not_found.html',
            redirection_query=escape(redirection_query))

@application.route("/", methods=["GET"])
def index():
    return render_template('index.html')
