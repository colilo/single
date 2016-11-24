//
// Created by chenchaq on 11/24/16.
//

#include <amqp.h>
#include <amqp_tcp_socket.h>

void *(*consumer)(void *argv)
{
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    char *hostname;
    int port;
    int status = amqp_socket_open(socket, hostname, port);
    char *vhost;
    int channel_max;
    int frame_max;
    int heartbeat;
    amqp_sasl_method_enum sasl_method = AMQP_SASL_METHOD_UNDEFINED;
    char *username;
    char *passwork;
    amqp_rpc_reply_t login_status = amqp_login(conn, vhost, channel_max, frame_max, heartbeat, sasl_method, username, passwork);
    amqp_channel_t channel;
    amqp_channel_open(conn, channel);
    amqp_bytes_t queue_bytes;
    amqp_boolean_t passive;
    amqp_boolean_t durable;
    amqp_boolean_t exclusive;
    amqp_boolean_t auto_delete;
    amqp_table_t arguments;
    amqp_queue_declare_ok_t *qdeclare_status = amqp_queue_declare(conn, channel, queue_bytes, passive, durable, exclusive, auto_delete, arguments);
    amqp_bytes_t exchange_bytes;
    amqp_bytes_t routing_key_bytes;
    amqp_queue_bind_ok_t *qbind_status = amqp_queue_bind(conn, channel, queue_bytes, exchange_bytes, routing_key_bytes, arguments);
    amqp_bytes_t consumer_tag;
    amqp_boolean_t no_local;
    amqp_boolean_t no_ack;
    amqp_basic_consume_ok_t *consume_status = amqp_basic_consume(conn, channel, queue_bytes, consumer_tag, no_local, no_ack, exclusive, arguments);
    
    for (;;) {
        amqp_maybe_release_buffers(conn);
        amqp_envelope_t *envelope;
        struct timeval *timeout;
        int flags;
        amqp_rpc_reply_t consumemsg_status = amqp_consume_message(conn, envelope, timeout, flags);
    }

    int code;
    amqp_rpc_reply_t chclose_status = amqp_channel_close(conn, channel, code);
    amqp_rpc_reply_t cnclose_status = amqp_connection_close(conn, code);
    int cndestroy_status = amqp_destroy_connection(conn);
}
