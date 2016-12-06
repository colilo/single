//
// Created by chenchaq on 11/25/16.
//

#ifndef RABBITMQ_TOOL_C_SINGLE_UTILS_H
#define RABBITMQ_TOOL_C_SINGLE_UTILS_H

#include <amqp.h>

void output_amqp_bytes(char *name, amqp_bytes_t *need_to_output);
void output_amqp_status(unsigned int code);
void output_amqp_rpc_status(char *name, amqp_rpc_reply_t rpc_status);
unsigned long getCurrentMicrosecond();
void microsecondSleep(unsigned long microseconds);

#endif //RABBITMQ_TOOL_C_SINGLE_UTILS_H
