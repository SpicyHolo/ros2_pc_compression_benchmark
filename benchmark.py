import argparse
import json
import os
import shlex
import signal
import subprocess
import time

from datetime import datetime
from pathlib import Path

DEBUG = True
class ROSLaunch:
    def __init__(self, config, basedir):
        self.dir = basedir
        self.name = config.get('name')
        self.pkg = config.get('launch_pkg') 
        self.launch_file = config.get('launch_file')
        self.launch_args_raw = config.get('launch_args')

    def getLaunchCommand(self, rosbag):
        launch_cmd = ['ros2', 'launch', self.pkg, self.launch_file]

        additional_topics = f"{rosbag.imu_topic} {rosbag.gps_topic}"
        args = [
            arg.format(
                bag_path=f"{self.dir}/compressed/{self.name}/{rosbag.name}",
                point_cloud_topic=rosbag.point_cloud_topic,
                stats_path=f"{self.dir}/compressed/{self.name}/stats",
                output_topic="/decompressed",
                additional_topics=additional_topics
            )
            for arg in self.launch_args_raw
        ]
        launch_cmd.extend(args)

        if DEBUG:
            print(f"[DEBUG] Launch {self.name}: {launch_cmd}")
        return launch_cmd

class ROSBag:
    def __init__(self, bag_config, basedir):
        if DEBUG:
            print(f"[DEBUG] Created ROSBag object.")
            print(bag_config)

        self.dir = basedir
        # Bag play variables
        self.path = bag_config.get('path')
        self.rate = str(bag_config.get('rate', 1.0))
        self.read_ahead_queue_size = str(bag_config.get('read_ahead_queue_size', 1000))

        # Topics
        self.point_cloud_topic = bag_config.get('point_cloud_topic')
        self.imu_topic = bag_config.get('imu_topic')
        self.gps_topic = bag_config.get('gps_topic')

        # TODO: Ground truth
        self.name = Path(self.path).stem
        self.post_delay = bag_config.get('post_delay', 5)
        
    def __repr__(self):
        return f"<ROSBag path={self.path} rate={self.rate} topic={self.point_cloud_topic}, imu:{self.imu_topic}, gps: {self.gps_topic}>"

    def launch(self, launch: ROSLaunch):
        bag_proc = None
        launch_proc = None
        try:
            bag_cmd = ['ros2', 'bag', 'play', self.path, '--rate', self.rate]
            bag_proc = subprocess.Popen(bag_cmd, 
                                        stdin=subprocess.DEVNULL, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE)

            launch_cmd = launch.getLaunchCommand(self)
            launch_proc = subprocess.Popen(launch_cmd, 
                                           stdin=subprocess.DEVNULL, 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE,
                                           preexec_fn=os.setsid)

            print(f"[INFO] Launched rosbag and launch file. Waiting for rosbag to finish...")
            bag_proc.wait()

            print(f"[INFO] Rosbag finished. Waiting {self.post_delay} seconds before shutting down launch.")
            time.sleep(self.post_delay)

            # Terminate the launch process group
            os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)
            print("[INFO] Launch file terminated.")

        except KeyboardInterrupt:
            print("[INFO] Interrupted. Cleaning up...")
            if bag_proc and bag_proc.poll() is None:
                bag_proc.terminate()
            if launch_proc and launch_proc.poll() is None:
                os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)

def run_benchmark(config_path):
    benchmark_dir = datetime.now().strftime("benchmark_%Y-%m-%d_%H-%M-%S")

    with open(config_path, 'r') as f:
        config = json.load(f)

    bags = [ROSBag(bag_config, benchmark_dir) for bag_config in config.get('bags')]
    compression_launches = [ROSLaunch(launch_config, benchmark_dir) for launch_config in config.get('compression')]

    for bag in bags:
        print("****************************")
        print(f"[INFO] Running preprocessing for bag: {bag.name}")
        
        for launch in compression_launches:
            print(f"[INFO] Running preprocessing: {launch.name}")
            bag.launch(launch)


def main():
    parser = argparse.ArgumentParser(description="Run rosbag + multiple launch file benchmarks from JSON config.")
    parser.add_argument('--config', required=True, help='Path to JSON benchmark config file')
    args = parser.parse_args()

    run_benchmark(args.config)

if __name__ == '__main__':
    main()
