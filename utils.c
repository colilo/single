//
// Created by chenchaq on 11/25/16.
//

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include "utils.h"


unsigned long getCurrentMicrosecond()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (unsigned long)tv.tv_sec * 1000000ul + (unsigned long)tv.tv_usec;
}

void microsecondSleep(unsigned long microseconds)
{
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1000 * microseconds;
    nanosleep(&ts, NULL);
}


void output_amqp_bytes(char *name, amqp_bytes_t *need_to_output)
{
    int i = 0;
    printf("%s len: %u, %s: \n", name, need_to_output->len, name);
    char *iter = need_to_output->bytes;
    for (i = 1; i <= need_to_output->len; i++) {
        printf("%c(%x)", *iter, *iter);
        if (i & 7 == 0) {
            printf("\n");
        }
        iter++;
    }
    printf("\n");
}

void output_amqp_status(unsigned int code)
{
    switch(code) {
        case AMQP_STATUS_OK:
//            printf("code(%d): %d, description: %s\n", code, 0x0, "Operation successful");
            break;
        case AMQP_STATUS_NO_MEMORY:
            printf("code(%d): %d, description: %s\n", code, -0x0001,  "Memory allocation failed");
            break;
        case AMQP_STATUS_BAD_AMQP_DATA:
            printf("code(%d): %d, description: %s\n", code, -0x0002, "Incorrect or corrupt data was received from the broker. This is a protocol error.");
            break;
        case AMQP_STATUS_UNKNOWN_CLASS :
            printf("code(%d): %d, description: %s\n", code, -0x0003, "An unknown AMQP class was received. This is a protocol error.");
            break;
        case AMQP_STATUS_UNKNOWN_METHOD :
            printf("code(%d): %d, description: %s\n", code, -0x0004, "An unknown AMQP method was received. This is a protocol error.");
            break;
        case AMQP_STATUS_HOSTNAME_RESOLUTION_FAILED:
            printf("code(%d): %d, description: %s\n", code, -0x0005, "Unable to resolve the hostname");
            break;
        case AMQP_STATUS_INCOMPATIBLE_AMQP_VERSION :
            printf("code(%d): %d, description: %s\n", code, -0x0006, "The broker advertised an incompaible AMQP version");
            break;
        case AMQP_STATUS_CONNECTION_CLOSED :
            printf("code(%d): %d, description: %s\n", code, -0x0007, "The connection to the broker has been closed");
            break;
        case AMQP_STATUS_BAD_URL :
            printf("code(%d): %d, description: %s\n", code, -0x0008, "malformed AMQP URL");
            break;
        case AMQP_STATUS_SOCKET_ERROR :
            printf("code(%d): %d, description: %s\n", code, -0x0009, "A socket error occurred");
            break;
        case AMQP_STATUS_INVALID_PARAMETER :
            printf("code(%d): %d, description: %s\n", code, -0x000A, "An invalid parameter was passed into the function");
            break;
        case AMQP_STATUS_TABLE_TOO_BIG :
            printf("code(%d): %d, description: %s\n", code, -0x000B, "The amqp_table_t object cannot be serialized because the output buffer is too small");
            break;
        case AMQP_STATUS_WRONG_METHOD :
            printf("code(%d): %d, description: %s\n", code, -0x000C, "The wrong method was received");
            break;
        case AMQP_STATUS_TIMEOUT :
            printf("code(%d): %d, description: %s\n", code, -0x000D, "Operation timed out");
            break;
        case AMQP_STATUS_TIMER_FAILURE :
            printf("code(%d): %d, description: %s\n", code, -0x000E, "The underlying system timer facility failed");
            break;
        case AMQP_STATUS_HEARTBEAT_TIMEOUT :
            printf("code(%d): %d, description: %s\n", code, -0x000F, "Timed out waiting for heartbeat");
            break;
        case AMQP_STATUS_UNEXPECTED_STATE :
            printf("code(%d): %d, description: %s\n", code, -0x0010, "Unexpected protocol state");
            break;
        case AMQP_STATUS_SOCKET_CLOSED :
            printf("code(%d): %d, description: %s\n", code, -0x0011, "Underlying socket is closed");
            break;
        case AMQP_STATUS_SOCKET_INUSE :
            printf("code(%d): %d, description: %s\n", code, -0x0012, "Underlying socket is already open");
            break;
        case AMQP_STATUS_BROKER_UNSUPPORTED_SASL_METHOD :
            printf("code(%d): %d, description: %s\n", code, -0x0013, "Broker does not support the requested SASL mechanism");
            break;
        case AMQP_STATUS_UNSUPPORTED :
            printf("code(%d): %d, description: %s\n", code, -0x0014, "Parameter is unsupported in this version");
            break;
        case _AMQP_STATUS_NEXT_VALUE :
            printf("code(%d): %d, description: %s\n", code, -0x0015, "Internal value");
            break;
        case AMQP_STATUS_TCP_ERROR :
            printf("code(%d): %d, description: %s\n", code, -0x0100, "A generic TCP error occurred");
            break;
        case AMQP_STATUS_TCP_SOCKETLIB_INIT_ERROR :
            printf("code(%d): %d, description: %s\n", code, -0x0101, "An error occurred trying to initialize the socket library");
            break;
        case _AMQP_STATUS_TCP_NEXT_VALUE :
            printf("code(%d): %d, description: %s\n", code, -0x0102, "Internal value");
            break;
        case AMQP_STATUS_SSL_ERROR :
            printf("code(%d): %d, description: %s\n", code, -0x0200, "A generic SSL error occurred.");
            break;
        case AMQP_STATUS_SSL_HOSTNAME_VERIFY_FAILED:
            printf("code(%d): %d, description: %s\n", code, -0x0201, "SSL validation of hostname against peer certificate failed");
            break;
        case AMQP_STATUS_SSL_PEER_VERIFY_FAILED :
            printf("code(%d): %d, description: %s\n", code, -0x0202, "SSL validation of peer certificate failed.");
            break;
        case AMQP_STATUS_SSL_CONNECTION_FAILED :
            printf("code(%d): %d, description: %s\n", code, -0x0203, "SSL handshake failed.");
            break;
        case _AMQP_STATUS_SSL_NEXT_VALUE :
            printf("code(%d): %d, description: %s\n", code, -0x0204, "Internal value");
            break;
        default:
            printf("code(%d): %d, description: %s\n", code, code, "Unknown");
    }
}


void output_amqp_rpc_status(char *name, amqp_rpc_reply_t rpc_status) {
    printf("%s.reply.decoded: %#x\n", name, rpc_status.reply.decoded);
    printf("%s.reply.id: %u\n", name, rpc_status.reply.id);
    switch (rpc_status.reply_type) {
        case AMQP_RESPONSE_NONE:
            output_amqp_status(rpc_status.library_error);
            break;
        case AMQP_RESPONSE_NORMAL:
            break;
        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            printf("%s\n", amqp_error_string2(rpc_status.library_error));
            break;
        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (rpc_status.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    amqp_connection_close_t *m = (amqp_connection_close_t *) rpc_status.reply.decoded;
                    printf("server connection error %d, message: %.*s", m->reply_code, (int) m->reply_text.len,
                           (char *) m->reply_text.bytes);
                    break;
                }
                case AMQP_CHANNEL_CLOSE_METHOD: {
                    amqp_channel_close_t *m = (amqp_channel_close_t *) rpc_status.reply.decoded;
                    printf("server channel error %d, message: %.*s", m->reply_code, (int) m->reply_text.len,
                           (char *) m->reply_text.bytes);
                    break;
                }
                default: {
                    printf("unknown server error, method id 0x%08X", rpc_status.reply.id);
                    break;
                }
            }
            break;
        default:
            abort();
    }
}

void output_amqp_envelope_info(amqp_envelope_t envelope) {
    output_amqp_bytes("envelope.consumer_tag", &(envelope.consumer_tag));
    printf("envelope.channel: %u\n", envelope.channel);
    printf("envelope.delivery_tag: %lu\n", envelope.delivery_tag);
    output_amqp_bytes("envelope.exchange", &(envelope.exchange));
    output_amqp_bytes("envelope.message.body", &(envelope.message.body));
    envelope.message.pool;
    printf("envelope.message.properties._flags: %u(%x)\n", envelope.message.properties._flags,
           envelope.message.properties._flags);
    output_amqp_bytes("envelope.message.properties.app_id", &(envelope.message.properties.app_id));
    output_amqp_bytes("envelope.message.properties.cluster_id", &(envelope.message.properties.cluster_id));
    output_amqp_bytes("envelope.message.properties.content_encoding", &(envelope.message.properties.content_encoding));
    output_amqp_bytes("envelope.message.properties.content_type", &(envelope.message.properties.content_type));
    output_amqp_bytes("envelope.message.properties.correlation_id", &(envelope.message.properties.correlation_id));
    printf("envelope.message.properties.delivery_mode: %c(%x)\n", envelope.message.properties.delivery_mode,
           envelope.message.properties.delivery_mode);
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
}