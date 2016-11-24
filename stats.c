//
// Created by chenchaq on 11/24/16.
//

#include <sys/time.h>
#include <stddef.h>
#include <stdio.h>
#include "stats.h"

void printFinal(stats_t stats) {

    struct timeval tv;
    gettimeofday(&tv, NULL);
    long now = tv.tv_sec * 1000000ul + tv.tv_usec;
    long elapsed = now - stats.startTime;

    printf("sending, reate avg: %6.2f msgs/s", stats.sendCountTotal * 1000.0 / elapsed);

    if (elapsed > 0) {
        printf("recving rate avg: %6.2f msgs/s", stats.recvCountTotal * 1000.0 / elapsed);
    }
}
