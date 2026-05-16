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
# Use lazy symbol binding so that unused rclcpp internal symbols (e.g.
# IntraProcessManager::add_publisher) are not resolved at startup.
# This makes the binary ABI-compatible across ROS 2 distros as long as
# intra-process comms is disabled at runtime (see graph_check_node.cpp).
# NOTE: do NOT statically link libstdc++/libgcc — mixing a private static
# libstdc++ with the system libstdc++ pulled in by librclcpp.so causes
# duplicate type_info registrations and an abort() at startup.
target_link_options(graph_watcher PRIVATE
  -Wl,-z,lazy
)

install(TARGETS graph_watcher DESTINATION lib/${PROJECT_NAME})

ament_package()

