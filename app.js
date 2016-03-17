var r = require('rethinkdb');
var when = require('when');
var fs = require('fs');
var path = require('path');
var r = require('rethinkdb');
var http = require('http');
var request = require('request');

var express = require('express');
var bodyParser = require('body-parser');
app = express();
app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

 function onError(err) {
     console.log(err);
 }
 
 function unrecognizedError(event, data) {
     console.log("unrecognized event", JSON.stringify(event), JSON.stringify(data));
 }


function onConnect(conn) {
    var eventMap = {
      add: function (event) {
            console.log(event);
            r.db('test').table('messages').insert([event]).run(conn);
      },
      update: function (event) {
            console.log(event);
            r.db('test').table('messages').get(event.old_val.id).update(event.new_val).run(conn); 
      },
      delete:function (event) {
            console.log(event);
            r.db('test').table('messages').get(event.id).delete(); 
      },  
    };
    
    require("./eventHandler")('localhost:9092', "freewayEventTest", eventMap, unrecognizedError,__dirname + '/kafka-offsets', null);
}


request({
    url: "http://localhost:8083",
    method: "POST",
    json: true,   
    body: {
      topic: "EventClient"
    }
}, function (error, response, body){
    console.log(body);
    var sendEvent = require("./eventBroker")("localhost:9092","EventClient", null);
    startServer(sendEvent);
});

r.connect({ host: 'localhost', port: 28015 }).then(onConnect, onError);

function startServer(sendEvent) {
    app.post('/', function(req, res) {
    switch (req.headers.event) {
       case "add":
            sendEvent('add', req.body.data);  
            res.status(200).send('DONE');
            break;
       case "update":
            sendEvent('update', req.body.data);
            res.status(200).send('DONE');  
            break;
       case "delete":
            sendEvent('delete', req.body.data); 
            res.status(200).send('DONE'); 
            break;
        default:
            res.status(500).send('unrecognized');
            break;
    }
    
});
}

//start server
app.listen(8808);