//
// Created by chenchaq on 11/24/16.
//

#ifndef RABBITMQ_TOOL_C_SINGLE_STATS_H
#define RABBITMQ_TOOL_C_SINGLE_STATS_H
#include "types.h"

typedef struct {
    bool sendStatsEnabled;
    bool recvStatsEnabled;
    bool returnStatsEnabled;
    bool confirmStatsEnabled;
    int sendCountTotal;
    int recvCountTotal;
    unsigned long startTime;
    unsigned long lastStatsTime;
    unsigned long elapsedInterval;
    unsigned long elapsedTotal;
    unsigned long interval;

} stats_t;

void printFinal(stats_t stats);
#endif //RABBITMQ_TOOL_C_SINGLE_STATS_H
