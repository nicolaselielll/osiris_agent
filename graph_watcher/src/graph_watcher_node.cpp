#include <chrono>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/empty.hpp"

using namespace std::chrono_literals;

static constexpr auto WATCHDOG_TIMEOUT = std::chrono::seconds(15);

class GraphWatcher : public rclcpp::Node
{
public:
  GraphWatcher()
  : Node("osiris_graph_watcher")
  {
    pub_ = this->create_publisher<std_msgs::msg::Empty>(
      "/osiris/graph_changed", rclcpp::QoS(1).transient_local());

    thread_ = std::thread(&GraphWatcher::watch_loop, this);

    RCLCPP_INFO(this->get_logger(),
      "osiris_graph_watcher started - publishing to /osiris/graph_changed");
  }

  ~GraphWatcher()
  {
    stop_ = true;
    if (thread_.joinable()) {
      thread_.join();
    }
  }

private:
  void watch_loop()
  {
    while (!stop_ && rclcpp::ok()) {
      auto event = this->get_graph_event();

      this->wait_for_graph_change(event, WATCHDOG_TIMEOUT);

      if (!rclcpp::ok() || stop_) {
        break;
      }

      if (!event->check_and_clear()) {
        RCLCPP_DEBUG(this->get_logger(), "watchdog timeout - no graph change, skipping");
        continue;
      }

      auto msg = std_msgs::msg::Empty();
      pub_->publish(msg);

      RCLCPP_DEBUG(this->get_logger(), "graph changed - notified agent");
    }
  }

  rclcpp::Publisher<std_msgs::msg::Empty>::SharedPtr pub_;
  std::thread thread_;
  std::atomic<bool> stop_{false};
};

int main(int argc, char * argv[])
{
  rclcpp::init(argc, argv);
  auto node = std::make_shared<GraphWatcher>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}
