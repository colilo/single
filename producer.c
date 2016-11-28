//
// Created by chenchaq on 11/28/16.
//

#include <stdio.h>
#include <string.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <stdlib.h>
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
        amqp_rpc_reply_t rpc_status = amqp_get_rpc_reply(conn);
        output_amqp_rpc_status("channel open", rpc_status);
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
        output_amqp_rpc_status("amqp_consume_message", consumemsg_rpc_status);

        switch (consumemsg_rpc_status.reply_type) {
            case AMQP_RESPONSE_NONE:
                break;
            case AMQP_RESPONSE_NORMAL: {
                amqp_bytes_t *message = &(envelope.message.body);
                unsigned char integer[4] = {0};
                integer[3] = *((unsigned char *) message->bytes);
                integer[2] = *((unsigned char *) message->bytes + 1);
                integer[1] = *((unsigned char *) message->bytes + 2);
                integer[0] = *((unsigned char *) message->bytes + 3);
                int seq = *(int *) (integer);

                unsigned char integer64[8] = {0};
                integer64[7] = *((unsigned char *) message->bytes + 4);
                integer64[6] = *((unsigned char *) message->bytes + 4 + 1);
                integer64[5] = *((unsigned char *) message->bytes + 4 + 2);
                integer64[4] = *((unsigned char *) message->bytes + 4 + 3);
                integer64[3] = *((unsigned char *) message->bytes + 4 + 4);
                integer64[2] = *((unsigned char *) message->bytes + 4 + 5);
                integer64[1] = *((unsigned char *) message->bytes + 4 + 6);
                integer64[0] = *((unsigned char *) message->bytes + 4 + 7);
                unsigned long time = *((unsigned long *) (integer64));
                printf("seq: %d, time: %lu\n", seq, time);
                amqp_destroy_envelope(&envelope);
                break;
            }
            case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                break;
            case AMQP_RESPONSE_SERVER_EXCEPTION:
                break;
            default:
                abort();
        }
    }

    int code = AMQP_REPLY_SUCCESS;
    amqp_rpc_reply_t chclose_rpc_status = amqp_channel_close(conn, channel, code);
    amqp_rpc_reply_t cnclose_rpc_status = amqp_connection_close(conn, code);
    int cndestroy_status = amqp_destroy_connection(conn);

    return NULL;
}