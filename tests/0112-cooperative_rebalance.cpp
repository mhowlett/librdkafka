/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2020, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <iostream>
#include <map>
#include <cstring>
#include <cstdlib>
#include "testcpp.h"

/**
 * MH: what i'm currently using to debug with. Not finished.
 */


class ExampleRebalanceCb : public RdKafka::RebalanceCb {
private:
  static void part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions){
    for (unsigned int i = 0 ; i < partitions.size() ; i++)
      std::cerr << partitions[i]->topic() <<
	"[" << partitions[i]->partition() << "], ";
    std::cerr << "\n";
  }

public:
  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
		     RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions) {
    std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

    part_list_print(partitions);

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      consumer->incremental_assign(partitions);
//      partition_cnt = (int)partitions.size();
    } else {
      consumer->unassign();
//      partition_cnt = 0;
    }
//    eof_cnt = 0;
  }
};


static void no_subscribe_test(RdKafka::KafkaConsumer *c1, std::string &t1, std::string &t2) {
  std::vector<RdKafka::TopicPartition *> assignment;

  std::vector<RdKafka::TopicPartition *> toppars1;
  toppars1.push_back(RdKafka::TopicPartition::create(t1, 0,
                                                     RdKafka::Topic::OFFSET_BEGINNING));
  c1->incremental_assign(toppars1);
  c1->assignment(assignment);
  RdKafka::Message *m = c1->consume(5000);
  m = c1->consume(5000);

  c1->incremental_unassign(toppars1);
  c1->assignment(assignment);
  // m = c1->consume(5000);

  std::vector<RdKafka::TopicPartition *> toppars2;
  toppars2.push_back(RdKafka::TopicPartition::create(t2, 0,
                                                     RdKafka::Topic::OFFSET_BEGINNING));
  c1->incremental_assign(toppars2);
  c1->assignment(assignment);
  m = c1->consume(5000);

  c1->incremental_assign(toppars1);
  c1->assignment(assignment);
  m = c1->consume(5000);

  c1->incremental_unassign(toppars2);
  c1->incremental_unassign(toppars1);
}


extern "C" {
  int main_0112_cooperative_rebalance (int argc, char **argv) {
    int msgcnt = 1000;
    const int msgsize = 100;

    std::string topic1_str = Test::mk_topic_name("0112-cooperative_rebalance", 1);
    test_create_topic(NULL, topic1_str.c_str(), 1, 1);
    test_produce_msgs_easy_size(topic1_str.c_str(), 0, 0, msgcnt, msgsize);

    std::string topic2_str = Test::mk_topic_name("0112-cooperative_rebalance", 1);
    test_create_topic(NULL, topic2_str.c_str(), 1, 1);
    test_produce_msgs_easy_size(topic2_str.c_str(), 0, 0, msgcnt, msgsize);

    /* Create consumer 1 */
    RdKafka::Conf *conf;
    Test::conf_init(&conf, NULL, 20);
    Test::conf_set(conf, "group.id", topic1_str); // this is different every time.
    Test::conf_set(conf, "auto.offset.reset", "earliest");
    std::string bootstraps;
    if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
      Test::Fail("Failed to retrieve bootstrap.servers");
    ExampleRebalanceCb ex_rebalance_cb;
    std::string errstr;
    conf->set("rebalance_cb", &ex_rebalance_cb, errstr);
    RdKafka::KafkaConsumer *c1 = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!c1)
      Test::Fail("Failed to create KafkaConsumer: " + errstr);
    delete conf;

    no_subscribe_test(c1, topic1_str, topic2_str);

    c1->close();
    delete c1;
    return 0;

    /* Create consumer 2 */
    Test::conf_init(&conf, NULL, 20);
    Test::conf_set(conf, "group.id", topic1_str);
    if (conf->get("bootstrap.servers", bootstraps) != RdKafka::Conf::CONF_OK)
      Test::Fail("Failed to retrieve bootstrap.servers");
    RdKafka::KafkaConsumer *c2 = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!c2)
      Test::Fail("Failed to create KafkaConsumer: " + errstr);
    delete conf;

    /* Create topics */
    Test::create_topic(c1, topic1_str.c_str(), 1, 1);

    /*
    * Consumer #1 subscribe
    */
    std::vector<std::string> topics;
    topics.push_back(topic1_str);
    RdKafka::ErrorCode err;
    if ((err = c1->subscribe(topics)))
      Test::Fail("consumer 1 subscribe failed: " + RdKafka::err2str(err));

    /* Start consuming until EOF is reached, which indicates that we have an
     * assignment and any errors should have been reported. */
    bool run = true;
    int cnt = 0;
    while (run) {
      RdKafka::Message *msg = c1->consume(tmout_multip(1000));
      cnt += 1;
      if (cnt == 5) {
        /*
          * Consumer #2 subscribe
          */
        if ((err = c2->subscribe(topics)))
          Test::Fail("consumer 2 subscribe failed: " + RdKafka::err2str(err));
      }
      switch (msg->err())
        {
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR_NO_ERROR:
        default:
          // run = false;
          break;
        }
    }

    c1->close();
    delete c1;

    c2->close();
    delete c2;

    return 0;
  }
}
