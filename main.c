#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <sys/time.h>
#include "parameter.h"
#include "stats.h"
#include "consumer.h"
#include "producer.h"

char *optionHelpInfo[60] = { "show usage", /* ? */
                             "", /* @ */
                             "multi ack every", /* A */
                             "", /* B */
                             "producer message count", /* C */
                             "consumer message count", /* D */
                             "", /* E */
                             "", /* F */
                             "", /* G */
                             "consumer connection URI", /* H */
                             "", /* I */
                             "", /* J */
                             "use random routing key per message", /* K */
                             "", /* L */
                             "frame max", /* M */
                             "", /* N */
                             "", /* O */
                             "", /* P */
                             "channel prefetch count", /* Q */
                             "consumer rate limit", /* R */
                             "", /* S */
                             "", /* T */
                             "", /* U */
                             "", /* V */
                             "", /* W */
                             "", /* X */
                             "", /* Y */
                             "", /* Z */
                             "", /* [ */
                             "", /* \ */
                             "", /* ] */
                             "", /* ^ */
                             "", /* _ */
                             "", /* ` */
                             "auto ack", /* a */
                             "heartbeat interval", /* b */
                             "max unconfirmed publishes", /* c */
                             "", /* d */
                             "exchange name", /* e */
                             "message flag(persisten, immediate)", /* f */
                             "", /* g */
                             "producer connection URI", /* h */
                             "sampling interval in seconds", /* i */
                             "", /* j */
                             "routing key", /* k */
                             "latency limitation", /* l */
                             "producer tx size", /* m */
                             "consumer tx size", /* n */
                             "", /* o */
                             "allow use of predeclared objects", /* p */
                             "consumer prefetch count", /* q */
                             "producer rate limit", /* r */
                             "message size in bytes", /* s */
                             "exchange type", /* t */
                             "queue name", /* u */
                             "", /* v */
                             "", /* w */
                             "producer count", /* x */
                             "consumer count", /* y */
                             "run duration in seconds(unlimited by default)", /* z */ };

void init_param(parameter_t *ptr);

void usage() {

    char optarg;
    printf("usage: <program>\n");
    for(optarg = 0; optarg < 60; optarg++)
    {
        if (strcmp(optionHelpInfo[optarg], "")) { // if optionHelpInfo[optarg] != ""
            printf(" -%c <arg> %s\n", optarg + '?', optionHelpInfo[optarg]);
        } else {
            continue;
        }
    }

}


int readInt( char msg) {
    int ret = 0;
    ret = *((int *)msg);
    return ret;
}

#if 0
void *(*consumer_thread)(void *pstats)
{
    stats_t *stats = pstats;

    int status;
    amqp_socket_t *socket = NULL;
    struct amqp_connection_info ci;
    amqp_connection_state_t conn;

    amqp_default_connection_info(&ci);

    amqp_parse_url(curi, ci);

    conn = amqp_new_connection();
    if (ci.ssl) {
#ifdef WITH_SSL
        socket = amqp_ssl_socket_new(conn);
    if (!socket) {
      die("creating SSL/TLS socket");
    }
    if (amqp_cacert) {
      amqp_ssl_socket_set_cacert(socket, amqp_cacert);
    }
    if (amqp_key) {
      amqp_ssl_socket_set_key(socket, amqp_cert, amqp_key);
    }
#else
        die("librabbitmq was not built with SSL/TLS support");
#endif
    } else {
        socket = amqp_tcp_socket_new(conn);
    }
    status = amqp_socket_open(socket, ci.host, ci.port);

    amqp_login(conn, ci.vhost, 0, 131072, heartbeat, AMQP_SASL_METHOD_PLAIN, ci.user, ci.password);
    amqp_channel_open(conn, 1);



    amqp_bytes_t queue_bytes = amqp_cstring_bytes(param._queueName);

    char *routing_key_rest;
    char *routing_key_token;
    char *routing_tmp;
    int routing_key_count = 0;

    /* Declare the queue as auto-delete.  */
    amqp_queue_declare_ok_t *res = amqp_queue_declare(conn, 1, queue_bytes, 0, 0, 0, 1, amqp_empty_table);
    amqp_queue_bind(conn, 1, queue_bytes, amqp_cstring_bytes(param._exchangeName), amqp_cstring_bytes(param._routingKey), amqp_empty_table);
    amqp_basic_consume(conn, 1, queue_bytes, amqp_empty_bytes, 0, no_ack, 0, amqp_empty_table);

    stats->lastStatsTime = stats->startTime = getCurrentMicrosecond()

    unsigned long now;

    amqp_frame_t frame;

    for (;;) {
        amqp_rpc_reply_t ret;
        amqp_envelope_t envelope;

        now = getCurrentMicrosecond();
        stats->elapsedInterval = now - stats->lastStatsTime;
        if (stats->elapsedInterval >= stats->interval) {
            stats->elapsedTotal += stats->elapsedInterval;
            report(now);
            reset(now);
        }

        amqp_maybe_release_buffers(conn);
        ret = amqp_consume_message(conn, &envelope, NULL, 0);

        if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
            if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type &&
                AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
                int r = amqp_simple_wait_frame(conn, &frame);
                if (AMQP_STATUS_OK != (amqp_status_enum)r) {
                    return NULL;
                }

                if (AMQP_FRAME_METHOD == frame.frame_type){
                    switch(frame.payload.method.id) {
                        case AMQP_BASIC_ACK_METHOD:
                            break;
                        case AMQP_BASIC_RETURN_METHOD:
                        {
                            amqp_message_t message;
                            ret = amqp_read_message(conn, frame.channel, &message, 0);
                            if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
                                return NULL;
                            }

                            readInt();
                            readUnsignedLong();
                            amqp_destroy_message(&message);
                            break;
                        }
                        case AMQP_CHANNEL_CLOSE_METHOD:
                            return NULL;

                        case AMQP_CONNECTION_CLOSE_METHOD:
                            return NULL;
                        default:
                            fprintf(stderr ,"An unexpected method was received %u\n", frame.payload.method.id);
                            return NULL;
                    }
                }
            }
        } else {
            amqp_destroy_envelope(&envelope);
        }

        stats->recvCountTotal++;
    }




}

void *(*producer_thread)(void *pstats)
{
    stats_t *stats = pstats;
}
#endif

int main(int argc, char *argv[]) {
    int optChar;
    int frameMax = 0;
    int heartbeat = 0;
    int samplingInterval = 1;
    char *curi = "amqp://localhost";
    char *puri = "amqp://localhost";
    parameter_t param;
    size_t len;

    if (argc < 2) {
        usage();
        return 2;
    }

    init_param(&param);
    while ((optChar = getopt(argc, argv, "t:e:u:k:Ki:r:R:x:y:m:n:c:l:aA:Q:q:s:z:C:D:f:M:b:ph:H:")) != -1)
    {
        switch (optChar)
        {
            case 't':
                param._exchangeType = optarg;
                break;
            case 'e':
                param._exchangeName = optarg;
                break;
            case 'u':
                param._queueName = optarg;
                break;
            case 'k':
                param._routingKey = optarg;
                break;
            case 'K':
                param._randomRoutingKey = true;
                break;
            case 'i':
                samplingInterval = atoi(optarg);
                break;
            case 'r':
                param._producerRateLimit = atof(optarg);
                break;
            case 'R':
                param._consumerRateLimit = atof(optarg);
                break;
            case 'x':
                param._producerCount = atoi(optarg);
                break;
            case 'y':
                param._consumerCount = atoi(optarg);
                break;
            case 'm':
                param._producerTxSize = atoi(optarg);
                break;
            case 'n':
                param._consumerTxSize = atoi(optarg);
                break;
            case 'c':
                param._confirm = atoi(optarg);
                break;
            case 'l':
                param._latencyLimitation= atoi(optarg);
                break;
            case 'a':
                param._autoAck = true;
                break;
            case 'A':
                param._multiAckEvery = atoi(optarg);
                break;
            case 'Q':
                param._channelPrefetch = atoi(optarg);
                break;
            case 'q':
                param._consumerPrefetch = atoi(optarg);
                break;
            case 's':
                param._minMsgSize= atoi(optarg);
                break;
            case 'z':
                param._timeLimit = atoi(optarg);
                break;
            case 'C':
                param._producerMsgCount = atoi(optarg);
                break;
            case 'D':
                param._consumerMsgCount = atoi(optarg);
                break;
            case 'f':
                if (strcmp("mandatory", optarg) == 0) {
                    param._flagMandatory = true;
                } else if (strcmp("immediate", optarg) == 0) {
                    param._flagImmediate = true;
                } else if (strcmp("persistent", optarg) == 0) {
                    param._flagPersistent = true;
                } else {

                }
                break;
            case 'M':
                param._frameMax = atoi(optarg);
                break;
            case 'b':
                param._heartbeat = atoi(optarg);
                break;
            case 'p':
                param._predeclared = true;
                break;
            case 'h':
                len = strlen(optarg);
                param._curi = malloc( len * sizeof(char) + 1);
                if (param._curi == NULL) {
                    printf("malloc failed\n");
                    return 1;
                }
                strncpy(param._curi, optarg, strlen(optarg));
                *(param._curi + len) = '\0';
                break;
            case 'H':
                len = strlen(optarg);
                param._puri = malloc( len * sizeof(char) + 1);
                if (param._puri == NULL) {
                    printf("malloc failed\n");
                    return 1;
                }
                strncpy(param._puri, optarg, strlen(optarg));
                *(param._puri + len) = '\0';
                break;
            case '?':
                if (optopt != '?')
                {
                    printf("Options -%c should specify %s\n", optopt, optionHelpInfo[optopt - '?']);
                }
                else
                {
                    usage();
                }

                break;
            default:
                abort();
        }
    }

    stats_t stats;
    void *targv[2];
    targv[0] = &param;
    pthread_t ctid;
    pthread_t ptid;
    if (param._consumerCount == 1) {
        pthread_create(&ctid, NULL, &consumer, targv);
//        consumer((void *)targv);
    }
    if (param._producerCount == 1) {
        pthread_create(&ptid, NULL, &producer, targv);
//        producer((void *)targv);
    }

    if (param._consumerCount == 1) {
        pthread_join(ctid, NULL);
//        consumer((void *)targv);
    }
    if (param._producerCount == 1) {
        pthread_join(ptid, NULL);;
//        producer((void *)targv);
    }

    return 0;
}

void init_param(parameter_t *ptr) {
    ptr->_exchangeType = "direct";
    ptr->_exchangeName = ptr->_exchangeType;
    ptr->_queueName = "";
    ptr->_routingKey = NULL;
    ptr->_randomRoutingKey = false;
    ptr->_producerRateLimit = 0.0;
    ptr->_consumerRateLimit = 0.0;
    ptr->_producerCount = 1;
    ptr->_consumerCount = 1;
    ptr->_producerTxSize = 0;
    ptr->_consumerTxSize = 0;
    ptr->_confirm = -1;
    ptr->_latencyLimitation = 0;
    ptr->_autoAck = false;
    ptr->_multiAckEvery = 0;
    ptr->_channelPrefetch = 0;
    ptr->_consumerPrefetch = 0;
    ptr->_minMsgSize = 0;
    ptr->_timeLimit = 0;
    ptr->_producerMsgCount = 0;
    ptr->_consumerMsgCount = 0;
    ptr->_flagMandatory = false;
    ptr->_flagPersistent = false;
    ptr->_flagImmediate = false;
    ptr->_predeclared = false;
    ptr->_frameMax = 0;
    ptr->_heartbeat = 0;
}