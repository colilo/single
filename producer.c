//
// Created by chenchaq on 11/28/16.
//

#include <stdio.h>
#include <string.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <stdlib.h>
#include <pthread.h>
#include <zconf.h>
#include "parameter.h"
#include "utils.h"

void *(producer)(void *argv)
{
    void **p = argv;
    parameter_t param = *(parameter_t *)p[0];
    amqp_connection_state_t conn             = amqp_new_connection();
    amqp_socket_t *socket                    = amqp_tcp_socket_new(conn);
    char *hostname = "localhost";
    int port = 5672;


    struct amqp_connection_info ci;

    amqp_default_connection_info(&ci);

    int ret = amqp_parse_url(param._puri, &ci);
    if (ret == AMQP_STATUS_BAD_URL) {
        printf("producer bad URL\n");
        return NULL;
    }


    int status                               = amqp_socket_open(socket, ci.host, ci.port);
    char *vhost = "/";
    int channel_max = 10;
    int frame_max = 131072;
    int heartbeat = 0;
    amqp_sasl_method_enum sasl_method = AMQP_SASL_METHOD_PLAIN;
    char *username = "guest";
    char *password = "guest";
    amqp_rpc_reply_t login_rpc_status            = amqp_login(conn, vhost, channel_max, frame_max, heartbeat, sasl_method, username, password);
    amqp_channel_t channel = 1;
    amqp_channel_open_ok_t *chopen_status        = amqp_channel_open(conn, channel);
    if (chopen_status == NULL) {
        printf("channel open failed\n");
        amqp_rpc_reply_t rpc_status = amqp_get_rpc_reply(conn);
        output_amqp_rpc_status("channel open", rpc_status);
        return NULL;
    }

    amqp_basic_properties_t props = {0};

    int i = 0;
    printf("channel id len: %d, channel bytes(hex byte): \n", chopen_status->channel_id.len);
    char *iter = chopen_status->channel_id.bytes;
    for (i = 1; i <= chopen_status->channel_id.len; i++) {
        printf("%c(%x) ", *iter, *iter);
        if (i & 7 == 0) {
            printf("\n");
        }
        iter++;
    }
    printf("\n");

    amqp_bytes_t queue_bytes;
    if (strcmp(param._queueName, "")) {
        queue_bytes = amqp_cstring_bytes(param._queueName);
    } else {
        queue_bytes.len = amqp_empty_bytes.len;
        queue_bytes.bytes = amqp_empty_bytes.bytes;
    }

    amqp_boolean_t passive = 0;
    amqp_boolean_t durable = 0;
    amqp_boolean_t exclusive = 0;
    amqp_boolean_t auto_delete = 1;
    amqp_table_t arguments;
    arguments.num_entries = amqp_empty_table.num_entries;
    arguments.entries = amqp_empty_table.entries;


    amqp_bytes_t exchange_name_bytes = amqp_cstring_bytes(param._exchangeName);
    amqp_bytes_t exchange_type_bytes = amqp_cstring_bytes(param._exchangeType);
    amqp_boolean_t internal = 0;
    amqp_exchange_declare_ok_t *edeclare_status = amqp_exchange_declare(conn, channel, exchange_name_bytes, exchange_type_bytes, passive, durable, 0, internal, arguments);
    if (edeclare_status == NULL) {
        printf("exchange declare failed\n");
        return NULL;
    }

    amqp_queue_declare_ok_t *qdeclare_status = amqp_queue_declare(conn, channel, queue_bytes, passive, durable, exclusive, auto_delete, arguments);
    if (qdeclare_status == NULL) {
        printf("queue declare failed\n");
        return NULL;
    }

    printf("queue name len: %d, queue name bytes: \n", qdeclare_status->queue.len);
    iter = qdeclare_status->queue.bytes;
    for (i = 1; i <= qdeclare_status->queue.len; i++) {
        printf("%c(%x)", *iter, *iter);
        if (i & 7 == 0) {
            printf("\n");
        }
        iter++;
    }
    printf("\n");
    printf("producer_count: %u, message_count: %u\n", qdeclare_status->consumer_count, qdeclare_status->message_count);

    amqp_bytes_t exchange_bytes = amqp_cstring_bytes(param._exchangeName);
    amqp_bytes_t routing_key_bytes = amqp_cstring_bytes(param._routingKey);
    amqp_queue_bind_ok_t *qbind_status       = amqp_queue_bind(conn, channel, queue_bytes, exchange_bytes, routing_key_bytes, arguments);
    if (qbind_status == NULL) {
        printf("queue bind failed\n");
        return NULL;
    }

    printf("queue bind result: demmy mean nothing: %c(%x)\n", qbind_status->dummy, qbind_status->dummy);

    amqp_bytes_t producer_tag;
    producer_tag.len = amqp_empty_bytes.len;
    producer_tag.bytes = amqp_empty_bytes.bytes;
    amqp_boolean_t no_local = 0;
    amqp_boolean_t no_ack = 1;


    size_t min_msg_size = param._minMsgSize;
    unsigned char *message = malloc(min_msg_size * sizeof(char));

    for (i = 0; i < min_msg_size; i++) {
        message[i] = i & 0xff;
    }

    amqp_bytes_t message_bytes;
    message_bytes.len = min_msg_size;
    message_bytes.bytes = message;





    unsigned long now;
    unsigned long startTime;
    startTime = now = getCurrentMicrosecond();
    unsigned long lastStatsTime = startTime;
    unsigned long msgCount = 0;
    int totalMsgCount = 0;
    while ((param._timeLimit == 0 || now < startTime + param._timeLimit * 1000000ul) && (param._producerMsgCount == 0 || msgCount < param._producerMsgCount)) {

        unsigned long elapsed = now - lastStatsTime;
        lastStatsTime = now;

        unsigned long pause = param._producerRateLimit == 0.0f ? 0.0f : (msgCount * 1000000.0 / param._producerRateLimit - elapsed);
        if (pause > 0) {
            microsecondSleep(pause);
        }
        msgCount = 0;
               
        unsigned long timestamp = getCurrentMicrosecond();
        message[0] = (unsigned char)(totalMsgCount >> 24 & 0xff);
        message[1] = (unsigned char)(totalMsgCount >> 16 & 0xff);
        message[2] = (unsigned char)(totalMsgCount >> 8  & 0xff);
        message[3] = (unsigned char)(totalMsgCount       & 0xff);
        message[4] =  (unsigned char)(timestamp >> 56 & 0xff);
        message[5] =  (unsigned char)(timestamp >> 48 & 0xff);
        message[6] =  (unsigned char)(timestamp >> 40 & 0xff);
        message[7] =  (unsigned char)(timestamp >> 32 & 0xff);
        message[8] =  (unsigned char)(timestamp >> 24 & 0xff);
        message[9] =  (unsigned char)(timestamp >> 16 & 0xff);
        message[10] = (unsigned char)(timestamp >> 8  & 0xff);
        message[11] = (unsigned char)(timestamp       & 0xff);



        int ret = amqp_basic_publish(conn, channel, exchange_bytes, routing_key_bytes, 0, 0, NULL, message_bytes);
//        output_amqp_status(ret);

        msgCount++;
        totalMsgCount++;
        now = getCurrentMicrosecond();
    }

    unsigned long endTime = getCurrentMicrosecond();
    printf("msg count: %d, all time: %lu\n", totalMsgCount, (endTime - startTime) );
    printf("msg count: %d, avg rate: %fmsg/s\n", totalMsgCount, totalMsgCount * 1000000.0 / (endTime - startTime) );

//    sleep(5);

    int code = AMQP_REPLY_SUCCESS;
    amqp_rpc_reply_t chclose_rpc_status = amqp_channel_close(conn, channel, code);
    amqp_rpc_reply_t cnclose_rpc_status = amqp_connection_close(conn, code);
    int cndestroy_status = amqp_destroy_connection(conn);

    return NULL;
}