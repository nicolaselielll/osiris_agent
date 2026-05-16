#include <chrono>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/empty.hpp"

using namespace std::chrono_literals;

static constexpr auto WATCHDOG_TIMEOUT  = std::chrono::seconds(15);
static constexpr auto DEBOUNCE_DURATION = std::chrono::milliseconds(200);

class GraphWatcher : public rclcpp::Node
{
public:
  GraphWatcher()
  : Node("osiris_graph_watcher",
         rclcpp::NodeOptions().use_intra_process_comms(false))
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
    // Arm the event ONCE before entering the loop so we never miss a change
    // that arrives between check_and_clear() and the next get_graph_event().
    auto event = this->get_graph_event();

    while (!stop_ && rclcpp::ok()) {
      this->wait_for_graph_change(event, WATCHDOG_TIMEOUT);

      if (!rclcpp::ok() || stop_) {
        break;
      }

      if (!event->check_and_clear()) {
        // Watchdog timeout with no change — re-use the same event object so
        // any change that arrived during the wait is not lost.
        RCLCPP_DEBUG(this->get_logger(), "watchdog timeout - no graph change, skipping");
        continue;
      }

      // Re-arm immediately BEFORE the debounce sleep so any graph changes
      // that arrive during the sleep are captured by the new event object.
      // This closes the race window AND coalesces burst changes into one
      // publish — keeping the DDS topic quiet during high-churn startup.
      event = this->get_graph_event();

      std::this_thread::sleep_for(DEBOUNCE_DURATION);

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
