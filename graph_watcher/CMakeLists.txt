cmake_minimum_required(VERSION 3.8)
project(graph_watcher)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)

add_executable(graph_watcher src/graph_watcher_node.cpp)
target_link_libraries(graph_watcher rclcpp::rclcpp ${std_msgs_TARGETS})
