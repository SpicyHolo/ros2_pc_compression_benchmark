import subprocess
import argparse
import time
import signal
import os
import shlex
import json

from datetime import datetime
from pathlib import Path

def run_benchmark(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)

        bag_path = config['bag']
        rate = str(config.get('rate', 1.0))
        read_ahead = str(config.get('read_ahead_queue_size', 1000))
        point_cloud_topic = config.get('point_cloud_topic', '')
        wait_after = config.get('wait_after', 5)
        benchmarks = config['benchmarks']

        for bench in benchmarks:
            name = bench['name']
            launch_pkg = bench['launch_pkg']
            launch_file = bench['launch_file']
            launch_args = bench.get('launch_args', '') 
            print(launch_args)

            print(f"[INFO] Running compression algorithm: {name}")
            
            run_rosbag_with_launch(bag_path, launch_pkg, launch_file, launch_args)


def run_rosbag_with_launch(bag_path, launch_pkg, launch_file, launch_args=None, wait_after=5):
    try:
        # Start ros2 bag play
        bag_cmd = ['ros2', 'bag', 'play', bag_path, '--rate', '0.5']
        bag_proc = subprocess.Popen(bag_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Build ros2 launch command
        launch_cmd = ['ros2', 'launch', launch_pkg, launch_file]
        if launch_args:
            launch_cmd.extend(shlex.split(launch_args))  # Parse space-separated args safely

        launch_proc = subprocess.Popen(launch_cmd, preexec_fn=os.setsid)

        print(f"[INFO] Launched rosbag and launch file. Waiting for rosbag to finish...")

        # Wait for the bag to finish
        bag_proc.wait()

        print(f"[INFO] Rosbag finished. Waiting {wait_after} seconds before shutting down launch.")
        # Wait additional seconds
        time.sleep(wait_after)

        # Terminate the launch process group
        os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)
        print("[INFO] Launch file terminated.")

    except KeyboardInterrupt:
        print("[INFO] Interrupted. Cleaning up...")
        if bag_proc.poll() is None:
            bag_proc.terminate()
        if launch_proc.poll() is None:
            os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)

def main():
    parser = argparse.ArgumentParser(description="Run rosbag + multiple launch file benchmarks from JSON config.")
    parser.add_argument('--config', required=True, help='Path to JSON benchmark config file')
    args = parser.parse_args()

    run_benchmark(args.config)
if __name__ == '__main__':
    main()
