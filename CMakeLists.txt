cmake_minimum_required(VERSION 3.6)
project(rabbitmq_tool_c_single)

set(SOURCE_FILES main.c parameter.h types.h stats.h stats.c consumer.c)
add_executable(rabbitmq_tool_c_single ${SOURCE_FILES})