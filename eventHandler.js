var kafka_native = require('kafka-native'); //kafka libary

/**
 * Handles listing for events over Kafka.
 * param {string} broker
 * The location of the zookeeper instance to run kafka
 * @param {string} topic
 * The topic to send event to
 * @param object events
 * An object where each property is a function that handles the property name event
 * @param {string} offset_directory
 * The directory where the offset will be stored. The file expected is <topic>-o.offset
 * @param {function(string)} log
 * function that log data will be sent to.
 */
module.exports =function (broker, topic, events, unrecognizedEventHandler, offset_directory, log) {
    //setup consumer
    var consumer = new kafka_native.Consumer({
        broker: broker,
        topic: topic,
        offset_directory: offset_directory,
        //callback to fire when messagea is recived
        receive_callback: function(data) {
            //uwrap the list into anther function
            data.messages.forEach(function(m) {
                if (log) { log('recived message on topic ' + m.topic + " with a partition of "+ m.partition + " with the offset of " + m.offset + " with this payload " + m.payload)};
                //parse the event object
                var eventData = JSON.parse(m.payload);
                //call the event function specified by the user in events
                if (events[eventData.event]) {
                    if(log) {log("Calling event " + eventData.event);}
                    events[eventData.event](eventData.data);
                } else {
                    //if the event is not in the events object call the unrecognizedEventHandler with the event name
                    if(log) {log("Unrecognized event" + eventData.event);}
                    unrecognizedEventHandler(eventData.event, eventData.data);
                }
            });
            //return a  promise
            return Promise.resolve();
        }
    });
    //start consumer
    consumer.start();
};