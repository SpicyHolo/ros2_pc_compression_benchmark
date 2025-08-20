#!/usr/bin/env python3
import argparse
import os

import rclpy
from rclpy.serialization import deserialize_message
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions

from geometry_msgs.msg import PoseStamped  # change if your topic has a different type

def extract_tum_from_ros2_bag(bag_path, topic_name="/gt", output_path=None):
    if output_path is None:
        output_path = os.path.splitext(bag_path)[0] + "_tum.txt"

    # ROS 2 bag reader setup
    storage_options = StorageOptions(uri=bag_path, storage_id='sqlite3')
    converter_options = ConverterOptions('', '')  # no conversion needed
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    # Get topics
    topic_types = reader.get_all_topics_and_types()
    type_dict = {t.name: t.type for t in topic_types}

    if topic_name not in type_dict:
        raise ValueError(f"Topic {topic_name} not found in bag. Available topics: {list(type_dict.keys())}")

    msg_type_str = type_dict[topic_name]
    # Dynamically import the message type
    import importlib
    print(msg_type_str)
    pkg_name, _, msg_name = msg_type_str.split('/')
    module = importlib.import_module(pkg_name + ".msg")
    msg_type = getattr(module, msg_name)

    with open(output_path, "w") as f:
        while reader.has_next():
            topic, data, t = reader.read_next()
            if topic != topic_name:
                continue
            msg = deserialize_message(data, msg_type)
            timestamp = msg.header.stamp.sec + 1e-9 * msg.header.stamp.nanosec
            pos = msg.pose.position
            ori = msg.pose.orientation
            f.write(f"{timestamp:.9f} {pos.x} {pos.y} {pos.z} {ori.x} {ori.y} {ori.z} {ori.w}\n")

    print(f"TUM trajectory saved to {output_path}")


if __name__ == "__main__":
    rclpy.init()
    parser = argparse.ArgumentParser(description="Convert ROS 2 bag poses to TUM trajectory format")
    parser.add_argument("bag_path", type=str, help="Path to the ROS 2 bag (.db3) file")
    parser.add_argument("--topic", type=str, default="/pose", help="ROS 2 topic to extract poses from")
    parser.add_argument("--output", type=str, help="Output TUM trajectory file path")
    args = parser.parse_args()

    extract_tum_from_ros2_bag(args.bag_path, args.topic, args.output)
    rclpy.shutdown()