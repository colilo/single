//
// Created by chenchaq on 11/24/16.
//

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "utils.h"
#include "parameter.h"

/*
 * void *argv[0] = param
 * */

void *(consumer)(void *argv)
{
    void **p = argv;
    parameter_t param = *(parameter_t *)p[0];
    amqp_connection_state_t conn             = amqp_new_connection();
    amqp_socket_t *socket                    = amqp_tcp_socket_new(conn);
    char *hostname = "localhost";
    int port = 5672;
    int status                               = amqp_socket_open(socket, hostname, port);
    char *vhost = "/";
    int channel_max = 10;
    int frame_max = 131072;
    int heartbeat = 0;
    amqp_sasl_method_enum sasl_method = AMQP_SASL_METHOD_PLAIN;
    char *username = "guest";
    char *passwork = "guest";
    amqp_rpc_reply_t login_rpc_status            = amqp_login(conn, vhost, channel_max, frame_max, heartbeat, sasl_method, username, passwork);
    amqp_channel_t channel = 1;
    amqp_channel_open_ok_t *chopen_status        = amqp_channel_open(conn, channel);
    if (chopen_status == NULL) {
        printf("channel open failed\n");
        return NULL;
    }

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
    printf("consumer_count: %u, message_count: %u\n", qdeclare_status->consumer_count, qdeclare_status->message_count);

    amqp_bytes_t exchange_bytes = amqp_cstring_bytes(param._exchangeName);
    amqp_bytes_t routing_key_bytes = amqp_cstring_bytes(param._routingKey);
    amqp_queue_bind_ok_t *qbind_status       = amqp_queue_bind(conn, channel, queue_bytes, exchange_bytes, routing_key_bytes, arguments);
    if (qbind_status == NULL) {
        printf("queue bind failed\n");
        return NULL;
    }

    printf("queue bind result: demmy mean nothing: %c(%x)\n", qbind_status->dummy, qbind_status->dummy);

    amqp_bytes_t consumer_tag;
    consumer_tag.len = amqp_empty_bytes.len;
    consumer_tag.bytes = amqp_empty_bytes.bytes;
    amqp_boolean_t no_local = 0;
    amqp_boolean_t no_ack = 1;
    amqp_basic_consume_ok_t *consume_status  = amqp_basic_consume(conn, channel, queue_bytes, consumer_tag, no_local, no_ack, exclusive, arguments);
    if (consume_status == NULL) {
        printf("basic consume failed\n");
        return NULL;
    }

    printf("consumer tag len: %u, consumer tag bytes: \n", consume_status->consumer_tag.len);
    iter = consume_status->consumer_tag.bytes;
    for (i = 1; i <= consume_status->consumer_tag.len; i++) {
        printf("%c(%x)", *iter, *iter);
        if (i & 7 == 0) {
            printf("\n");
        }
        iter++;
    }
    printf("\n");
    
    for (;;) {
        amqp_maybe_release_buffers(conn);
        amqp_envelope_t envelope;
        struct timeval *timeout = NULL;
        int flags = 0;
        amqp_rpc_reply_t consumemsg_rpc_status = amqp_consume_message(conn, &envelope, timeout, flags);

        printf("consumemsg_rpc_status.reply.decoded: %#x\n", consumemsg_rpc_status.reply.decoded);
        printf("consumemsg_rpc_status.reply.id: %u\n", consumemsg_rpc_status.reply.id);
        switch (consumemsg_rpc_status.reply_type) {
            case AMQP_RESPONSE_NONE:
                output_amqp_status(consumemsg_rpc_status.library_error);
                output_amqp_bytes("envelope.consumer_tag", &(envelope.consumer_tag));
                printf("envelope.channel: %u\n", envelope.channel);
                printf("envelope.delivery_tag: %lu\n", envelope.delivery_tag);
                output_amqp_bytes("envelope.exchange", &(envelope.exchange));
                output_amqp_bytes("envelope.message.body", &(envelope.message.body));
//                envelope.message.pool;
                printf("envelope.message.properties._flags: %u(%x)\n", envelope.message.properties._flags, envelope.message.properties._flags);
                output_amqp_bytes("envelope.message.properties.app_id", &(envelope.message.properties.app_id));
                output_amqp_bytes("envelope.message.properties.cluster_id", &(envelope.message.properties.cluster_id));
                output_amqp_bytes("envelope.message.properties.content_encoding", &(envelope.message.properties.content_encoding));
                output_amqp_bytes("envelope.message.properties.content_type", &(envelope.message.properties.content_type));
                output_amqp_bytes("envelope.message.properties.correlation_id", &(envelope.message.properties.correlation_id));
                printf("envelope.message.properties.delivery_mode: %c(%x)\n", envelope.message.properties.delivery_mode, envelope.message.properties.delivery_mode);
                output_amqp_bytes("envelope.message.properties.expiration", &(envelope.message.properties.expiration));
                printf("envelope.message.properties.headers.num_entries: %d\n", envelope.message.properties.headers.num_entries);
                printf("envelope.message.properties.headers.entries: %#x\n", envelope.message.properties.headers.entries);
                output_amqp_bytes("envelope.message.properties.message_id", &(envelope.message.properties.message_id));
                printf("envelope.message.properties.priority: %u\n", envelope.message.properties.priority);
                output_amqp_bytes("envelope.message.properties.reply_to", &(envelope.message.properties.reply_to));
                printf("envelope.message.properties.timestamp: %lu\n", envelope.message.properties.timestamp);
                output_amqp_bytes("envelope.message.properties.type", &(envelope.message.properties.type));
                output_amqp_bytes("envelope.message.properties.user_id", &(envelope.message.properties.user_id));
                printf("envelope.redelivered: %s\n", envelope.redelivered ? "true" : "false");
                output_amqp_bytes("envelope.routing_key", &(envelope.routing_key));
                break;
            case AMQP_RESPONSE_NORMAL:
                output_amqp_status(consumemsg_rpc_status.library_error);
                output_amqp_bytes("envelope.consumer_tag", &(envelope.consumer_tag));
                printf("envelope.channel: %u\n", envelope.channel);
                printf("envelope.delivery_tag: %lu\n", envelope.delivery_tag);
                output_amqp_bytes("envelope.exchange", &(envelope.exchange));
                output_amqp_bytes("envelope.message.body", &(envelope.message.body));
                envelope.message.pool;
                printf("envelope.message.properties._flags: %u(%x)\n", envelope.message.properties._flags, envelope.message.properties._flags);
                output_amqp_bytes("envelope.message.properties.app_id", &(envelope.message.properties.app_id));
                output_amqp_bytes("envelope.message.properties.cluster_id", &(envelope.message.properties.cluster_id));
                output_amqp_bytes("envelope.message.properties.content_encoding", &(envelope.message.properties.content_encoding));
                output_amqp_bytes("envelope.message.properties.content_type", &(envelope.message.properties.content_type));
                output_amqp_bytes("envelope.message.properties.correlation_id", &(envelope.message.properties.correlation_id));
                printf("envelope.message.properties.delivery_mode: %c(%x)\n", envelope.message.properties.delivery_mode, envelope.message.properties.delivery_mode);
                output_amqp_bytes("envelope.message.properties.expiration", &(envelope.message.properties.expiration));
                printf("envelope.message.properties.headers.num_entries: %d\n", envelope.message.properties.headers.num_entries);
                printf("envelope.message.properties.headers.entries: %#x\n", envelope.message.properties.headers.entries);
                output_amqp_bytes("envelope.message.properties.message_id", &(envelope.message.properties.message_id));
                printf("envelope.message.properties.priority: %u\n", envelope.message.properties.priority);
                output_amqp_bytes("envelope.message.properties.reply_to", &(envelope.message.properties.reply_to));
                printf("envelope.message.properties.timestamp: %lu\n", envelope.message.properties.timestamp);
                output_amqp_bytes("envelope.message.properties.type", &(envelope.message.properties.type));
                output_amqp_bytes("envelope.message.properties.user_id", &(envelope.message.properties.user_id));
                printf("envelope.redelivered: %s\n", envelope.redelivered ? "true" : "false");
                output_amqp_bytes("envelope.routing_key", &(envelope.routing_key));
                amqp_destroy_envelope(&envelope);
                break;
            case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                output_amqp_status(consumemsg_rpc_status.library_error);
                output_amqp_bytes("envelope.consumer_tag", &(envelope.consumer_tag));
                printf("envelope.channel: %u", envelope.channel);
                printf("envelope.delivery_tag: %lu", envelope.delivery_tag);
                output_amqp_bytes("envelope.exchange", &(envelope.exchange));
                output_amqp_bytes("envelope.message.body", &(envelope.message.body));
                envelope.message.pool;
                printf("envelope.message.properties._flags: %u(%x)", envelope.message.properties._flags, envelope.message.properties._flags);
                output_amqp_bytes("envelope.message.properties.app_id", &(envelope.message.properties.app_id));
                output_amqp_bytes("envelope.message.properties.cluster_id", &(envelope.message.properties.cluster_id));
                output_amqp_bytes("envelope.message.properties.content_encoding", &(envelope.message.properties.content_encoding));
                output_amqp_bytes("envelope.message.properties.content_type", &(envelope.message.properties.content_type));
                output_amqp_bytes("envelope.message.properties.correlation_id", &(envelope.message.properties.correlation_id));
                printf("envelope.message.properties.delivery_mode: %c(%x)", envelope.message.properties.delivery_mode, envelope.message.properties.delivery_mode);
                output_amqp_bytes("envelope.message.properties.expiration", &(envelope.message.properties.expiration));
                printf("envelope.message.properties.headers.num_entries: %d", envelope.message.properties.headers.num_entries);
                printf("envelope.message.properties.headers.entries: %#x", envelope.message.properties.headers.entries);
                output_amqp_bytes("envelope.message.properties.message_id", &(envelope.message.properties.message_id));
                printf("envelope.message.properties.priority: %u", envelope.message.properties.priority);
                output_amqp_bytes("envelope.message.properties.reply_to", &(envelope.message.properties.reply_to));
                printf("envelope.message.properties.timestamp: %lu", envelope.message.properties.timestamp);
                output_amqp_bytes("envelope.message.properties.type", &(envelope.message.properties.type));
                output_amqp_bytes("envelope.message.properties.user_id", &(envelope.message.properties.user_id));
                printf("envelope.redelivered: %s\n", envelope.redelivered ? "true" : "false");
                output_amqp_bytes("envelope.routing_key", &(envelope.routing_key));
                break;
            case AMQP_RESPONSE_SERVER_EXCEPTION:
                output_amqp_status(consumemsg_rpc_status.library_error);
                output_amqp_bytes("envelope.consumer_tag", &(envelope.consumer_tag));
                printf("envelope.channel: %u", envelope.channel);
                printf("envelope.delivery_tag: %lu", envelope.delivery_tag);
                output_amqp_bytes("envelope.exchange", &(envelope.exchange));
                output_amqp_bytes("envelope.message.body", &(envelope.message.body));
                envelope.message.pool;
                printf("envelope.message.properties._flags: %u(%x)", envelope.message.properties._flags, envelope.message.properties._flags);
                output_amqp_bytes("envelope.message.properties.app_id", &(envelope.message.properties.app_id));
                output_amqp_bytes("envelope.message.properties.cluster_id", &(envelope.message.properties.cluster_id));
                output_amqp_bytes("envelope.message.properties.content_encoding", &(envelope.message.properties.content_encoding));
                output_amqp_bytes("envelope.message.properties.content_type", &(envelope.message.properties.content_type));
                output_amqp_bytes("envelope.message.properties.correlation_id", &(envelope.message.properties.correlation_id));
                printf("envelope.message.properties.delivery_mode: %c(%x)", envelope.message.properties.delivery_mode, envelope.message.properties.delivery_mode);
                output_amqp_bytes("envelope.message.properties.expiration", &(envelope.message.properties.expiration));
                printf("envelope.message.properties.headers.num_entries: %d", envelope.message.properties.headers.num_entries);
                printf("envelope.message.properties.headers.entries: %#x", envelope.message.properties.headers.entries);
                output_amqp_bytes("envelope.message.properties.message_id", &(envelope.message.properties.message_id));
                printf("envelope.message.properties.priority: %u", envelope.message.properties.priority);
                output_amqp_bytes("envelope.message.properties.reply_to", &(envelope.message.properties.reply_to));
                printf("envelope.message.properties.timestamp: %lu", envelope.message.properties.timestamp);
                output_amqp_bytes("envelope.message.properties.type", &(envelope.message.properties.type));
                output_amqp_bytes("envelope.message.properties.user_id", &(envelope.message.properties.user_id));
                printf("envelope.redelivered: %s\n", envelope.redelivered ? "true" : "false");
                output_amqp_bytes("envelope.routing_key", &(envelope.routing_key));
                break;
            default:
                abort();
        };

    }

    int code = AMQP_REPLY_SUCCESS;
    amqp_rpc_reply_t chclose_rpc_status = amqp_channel_close(conn, channel, code);
    amqp_rpc_reply_t cnclose_rpc_status = amqp_connection_close(conn, code);
    int cndestroy_status = amqp_destroy_connection(conn);

    return NULL;
}


