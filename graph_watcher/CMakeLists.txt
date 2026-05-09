cmake_minimum_required(VERSION 3.8)
project(graph_watcher)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)

add_executable(graph_watcher src/graph_check_node.cpp)
ament_target_dependencies(graph_watcher rclcpp std_msgs)

install(TARGETS graph_watcher DESTINATION lib/${PROJECT_NAME})

ament_package()
