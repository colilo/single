//
// Created by chenchaq on 11/25/16.
//

#ifndef RABBITMQ_TOOL_C_SINGLE_UTILS_H
#define RABBITMQ_TOOL_C_SINGLE_UTILS_H

#include <amqp.h>

void output_amqp_bytes(char *name, amqp_bytes_t *need_to_output);
void output_amqp_status(unsigned int code);
#endif //RABBITMQ_TOOL_C_SINGLE_UTILS_H
