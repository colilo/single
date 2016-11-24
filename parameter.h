//
// Created by chenchaq on 11/24/16.
//

#ifndef RABBITMQ_TOOL_C_SINGLE_PARAMETER_H
#define RABBITMQ_TOOL_C_SINGLE_PARAMETER_H
#include <stddef.h>
#include "types.h"

typedef struct {
    long _confirm ;
    long _latencyLimitation ;
    int _consumerCount ;
    int _producerCount;
    int _consumerTxSize;
    int _producerTxSize ;
    int _channelPrefetch;
    int _consumerPrefetch ;
    int _minMsgSize ;
    int _timeLimit;
    float _producerRateLimit;
    float _consumerRateLimit;
    int _producerMsgCount ;
    int _consumerMsgCount;
    char *_exchangeName ;
    char *_exchangeType ;
    char *_queueName;
    char *_routingKey ;
    bool _randomRoutingKey;
//    std::set <std::string> _flags;
    bool _flagMandatory;
    bool _flagPersistent;
    bool _flagImmediate;

    int _multiAckEvery ;
    bool _autoAck ;
    bool _autoDelete ;
    bool _predeclared;
} parameter_t;
#endif //RABBITMQ_TOOL_C_SINGLE_PARAMETER_H
