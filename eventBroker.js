var kafka_native = require('kafka-native'); //kafka library

/**
 * Sets up Kafka producer
 * @param {string} broker
 * The location of the zookeeper instance to run kafka
 * @param {string} topic
 * The topic to send event to
 * @param {function(string)} log
 * function that log data will be sent to.
 * @return {function (string, object)} send
 * sends the event pased int he first argument with the data in the second to the configured topic
 */
module.exports = function (broker, topic, log) {
    
    //setup producer
    var producer = new kafka_native.Producer({
        broker: broker
    });
    
    //setup send function
    function send(event, data) {
        if(log) { log("sending event: " + event + " with data: " + JSON.stringify(data) + " to topic: " + topic); }
        producer.send(topic, 0, [
            JSON.stringify({
                event: event,
                data: data
            })
        ]);
    }
    
     return send;
};