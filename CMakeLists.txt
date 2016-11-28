cmake_minimum_required(VERSION 2.6)
project(rabbitmq_tool_c_single)

set(SOURCE_FILES main.c parameter.h types.h stats.h stats.c consumer.c utils.h utils.c consumer.h producer.c)


add_executable(rabbitmq_tool_c_single ${SOURCE_FILES})
target_link_libraries(rabbitmq_tool_c_single rabbitmq)