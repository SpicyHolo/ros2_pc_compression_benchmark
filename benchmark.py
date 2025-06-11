import argparse
import json
import os
import signal
import subprocess
import time
import pickle

from datetime import datetime
from pathlib import Path
from typing import Optional

DEBUG = True

class ROSBag:
    def __init__(self, bag_config, basedir):
        self.compressed_bags = {}

        if DEBUG:
            print(f"[DEBUG] Created ROSBag object.")
            print(bag_config)

        self.dir = basedir
        # Bag play variables
        self.path = bag_config.get('path')
        self.read_ahead_queue_size = str(bag_config.get('read_ahead_queue_size', 1000))

        # Topics
        self.point_cloud_topic = str(bag_config.get('point_cloud_topic', ''))
        self.imu_topic = str(bag_config.get('imu_topic', ''))
        self.gps_topic = str(bag_config.get('gps_topic', ''))

        # TODO: Ground truth
        self.name = Path(self.path).stem

    def __repr__(self):
        return f"<ROSBag path={self.path}, topic={self.point_cloud_topic}, imu:{self.imu_topic}, gps: {self.gps_topic}>"

    def add_compressed_bag(self, compression_name, bag_path):
        self.compressed_bags[compression_name] = bag_path


class Compression:
    def __init__(self, config, basedir):
        self.dir = basedir
        self.name = config.get('name')
        self.pkg = config.get('launch_pkg') 
        self.launch_file = config.get('launch_file')
        self.launch_args_raw = config.get('launch_args')
        self.bag_rate = str(config.get('bag_rate', 1.0))
        self.post_delay = config.get('post_delay', 5.0)

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

    def launch(self, rosbag: ROSBag):
        bag_proc = None
        launch_proc = None
        try:
            bag_cmd = ['ros2', 'bag', 'play', rosbag.path, '--rate', self.bag_rate]
            bag_proc = subprocess.Popen(bag_cmd, 
                                        stdin=subprocess.DEVNULL, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE)

            launch_cmd = self.getLaunchCommand(rosbag)
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

            rosbag.add_compressed_bag(self.name, f"{self.dir}/compressed/{self.name}/{rosbag.name}")

        except KeyboardInterrupt:
            print("[INFO] Interrupted. Cleaning up...")
            if bag_proc and bag_proc.poll() is None:
                bag_proc.terminate()
            if launch_proc and launch_proc.poll() is None:
                os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)

class Slam:
    def __init__(self, config, basedir):
        self.dir = basedir
        self.name = config.get('name')
        self.use_gps = config.get('gps', False)
        self.use_imu = config.get('imu', False)
        self.post_delay = config.get('post_delay', 5.0)

    def launch(self, rosbag : ROSBag, compression: Optional[Compression] = None):
        lidar_topic = "/compressed" if compression else rosbag.point_cloud_topic  
        if compression:
            lidar_topic = "/compressed"
            map_path = f"{self.dir}/maps/{rosbag.name}_{compression.name}_{self.name}.simplemap"
            traj_path = f"{self.dir}/traj/{rosbag.name}_{compression.name}_{self.name}.tum"
            bag_path = rosbag.compressed_bags[compression.name]
        else:
            lidar_topic = rosbag.point_cloud_topic
            map_path = f"{self.dir}/maps/{rosbag.name}_{self.name}.simplemap"
            traj_path = f"{self.dir}/traj/{rosbag.name}_{self.name}.tum"
            bag_path = rosbag.path
            

        env = os.environ.copy()
        
        # TODO tf base -> LIDAR/IMU/GPS
        env.update({
            "MOLA_LIDAR_TOPIC": lidar_topic,
            "MOLA_GENERATE_SIMPLEMAP": "true",
            "MOLA_SIMPLEMAP_OUTPUT": map_path,
            "MOLA_SAVE_TRAJECTORY": "true",
            "MOLA_TUM_TRAJECTORY_OUTPUT": traj_path,
            })

        if self.use_gps:
            env.update({
                "MOLA_GPS_TOPIC": rosbag.gps_topic,
                "MOLA_USE_FIXED_GPS_POSE": "true",
            })

        if self.use_imu:
            env.update({
                "MOLA_IMU_TOPIC": rosbag.imu_topic,
                "MOLA_USE_FIXED_IMU_POSE": "true",
            })
        
        launch_proc = None
        try:
            launch_cmd = [f'mola-lo-gui-rosbag2 {bag_path}']
            launch_proc = subprocess.Popen(launch_cmd, 
                                           stdin=subprocess.DEVNULL, 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE,
                                           preexec_fn=os.setsid)

            print(f"[INFO] Launching SLAM...")
            launch_proc.wait()

            print(f"[INFO] SLAM finished. Waiting {self.post_delay} seconds before shutting down.")
            time.sleep(self.post_delay)
             
        except KeyboardInterrupt:
            print("[INFO] Interrupted. Cleaning up...")
            if launch_proc and launch_proc.poll() is None:
                os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)

def load_benchmark_process_config(constructor, config, *args):
    return [constructor(constructor_config, *args) for constructor_config in config]

def run_benchmark(config_path, compression_data):
    benchmark_dir = datetime.now().strftime("benchmark_%Y-%m-%d_%H-%M-%S")

    with open(config_path, 'r') as f:
        config = json.load(f)

    if not compression_data:
        bags = load_benchmark_process_config(ROSBag, config.get('bags'), benchmark_dir)
        compression_launches = load_benchmark_process_config(Compression, config.get('compression'), benchmark_dir)

        for bag in bags:
            print("****************************")
            print(f"[INFO] Running preprocessing for bag: {bag.name}")
            
            for compression in compression_launches:
                print(f"[INFO] Running preprocessing: {compression.name}")
                compression.launch(bag)
        pickle_path = f"{benchmark_dir}/compression_data.pkl"
        with open(pickle_path, 'wb') as f:
            pickle.dump({'bags': bags, 'compression_launches': compression_launches, 'benchmark_dir': benchmark_dir}, f)

    else:
        with open(compression_data, 'rb') as f:
            data = pickle.load(f)

        bags = data['bags']
        compression_launches = data['compression_launches']
        benchmark_dir = data['benchmark_dir']
    
    slam_launches = load_benchmark_process_config(Slam, config.get('slam'), benchmark_dir)

    print(f"[INFO] Starting SLAM Benchmarks...")
    for bag in bags:
        for compression in compression_launches:
            for slam in slam_launches:
                slam.launch(bag, compression)

def load_pickle(path):
    if os.path.isfile(path):
        with open(path, 'rb') as f:
            data = pickle.load(f)
        print(f"Data loaded from {path}")
        return data
    else:
        raise FileNotFoundError(f"No file found at {path}")

def main():
    parser = argparse.ArgumentParser(description="Run rosbag + multiple launch file benchmarks from JSON config.")
    parser.add_argument('--config', required=True, help='Path to JSON benchmark config file')
    parser.add_argument('--load_compress', type=str, default="", help='Path to pickle')
    args = parser.parse_args()
    
    # Load compresion stage
    compression_data = None
    if args.load_compress:
        try:
            compression_data = load_pickle(args.load_compress)
            
            print("[INFO] Loaded compression stage")
        except Exception as e:
            print("[ERROR] Loading compression stage:", e)

    run_benchmark(args.config, compression_data=compression_data)

if __name__ == '__main__':
    main()
