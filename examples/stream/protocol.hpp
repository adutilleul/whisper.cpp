#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include "json.hpp"

#include <iostream>

#include <unistd.h>
#include <limits.h>

const std::string DEFAULT_BOOTSTRAP_SERVERS = "aci.crolard.fr:9092";
const std::string DEFAULT_KAFKA_TOPIC = "records-topic";

using json = nlohmann::json;

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    /* If message.err() is non-zero the message delivery failed permanently
     * for the message. */
    if (message.err())
      std::cerr << "% Message delivery failed: " << message.errstr()
                << std::endl;
    else
      std::cerr << "% Message delivered to topic " << message.topic_name()
                << " [" << message.partition() << "] at offset "
                << message.offset() << std::endl;
  }
};

static ExampleDeliveryReportCb ex_dr_cb;

void producer_send_message(RdKafka::Producer* producer, const std::string& json_str, const std::string& topic) {
    retry: 
        RdKafka::ErrorCode err = 
        producer->produce(
                            /* Topic name */
                            topic,
                            /* Any Partition */
                            RdKafka::Topic::PARTITION_UA,
                            /* Make a copy of the value */
                            RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                            /* Value */
                            const_cast<char*>(json_str.c_str()), json_str.size(),
                            /* Key */
                            NULL, 0,
                            /* Timestamp (defaults to current time) */
                            0,
                            /* Message headers, if any */
                            NULL,
                            /* Per-message opaque value passed to delivery report */
                            NULL);
        if (err != RdKafka::ERR_NO_ERROR) {
            std::cerr << "% Failed to produce to topic " << topic << ": " <<
                RdKafka::err2str(err) << std::endl; 
        }
        else if (err == RdKafka::ERR__QUEUE_FULL) { 
            std::cerr << "% Producer queue is full (" << producer->outq_len() << " messages awaiting delivery): try again" << std::endl;
            // If the internal queue is full, wait for messages to be delivered and then retry. 
            producer->poll(1000/*block for max 1000ms*/);
            goto retry;
        }
        else {
            std::cout << "% Enqueued message (" << json_str.size() << " bytes) " <<
                "for topic " << topic.c_str() << std::endl; 
                
        } // A producer application should continually serve the delivery report queue // by calling poll() at frequent intervals. producer->poll(0);
}

void build_and_send_message(RdKafka::Producer* producer, const std::string& topic, const char* text) {
    const std::string text_str = text;

    char hostname[HOST_NAME_MAX];
    char username[LOGIN_NAME_MAX];
    gethostname(hostname, HOST_NAME_MAX);
    getlogin_r(username, LOGIN_NAME_MAX);


    json j;
    j["user"] = std::string(username);
    j["hostname"] = std::string(hostname);
    j["timestamp"] = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    j["text"] = text_str;

    std::string json_str = j.dump();

    producer_send_message(producer, json_str, topic);
}

RdKafka::Producer* setup_kafka_producer(const char* bootstrap_servers) {
 // Create configuration object
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  std::string errstr;

  // Set bootstrap broker(s).
  conf->set("bootstrap.servers", bootstrap_servers, errstr);

  if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
    std::cerr << errstr << std::endl;
    exit(1);
  };
 
  // Create a producer instance.
  RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
 
  delete conf;

  return producer;
}


void terminate_producer(RdKafka::Producer* producer) {
    /* Wait for final messages to be delivered or fail.
    * flush() is an abstraction over poll() which
    * waits for all messages to be delivered. */
    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000 /* wait for max 10 seconds */);

    if (producer->outq_len() > 0)
        std::cerr << "% " << producer->outq_len()
                << " message(s) were not delivered" << std::endl;

    delete producer;
}