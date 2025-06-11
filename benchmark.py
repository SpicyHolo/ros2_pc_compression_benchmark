import argparse
import json
import os
import pickle
import signal
import subprocess
import time

from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Callable
from tqdm import tqdm

DEBUG = True

class ROSBag:
    def __init__(self, bag_config: Dict[str, Any], basedir : Path):
        self.compressed_bags: Dict[str, Path] = {}

        if DEBUG:
            print(f"[DEBUG] Created ROSBag object.")
            print(bag_config)

        self.dir = Path(basedir)
        self.path = Path(bag_config.get('path', ''))
        self.read_ahead_queue_size = str(bag_config.get('read_ahead_queue_size', 1000))

        self.point_cloud_topic = str(bag_config.get('point_cloud_topic', ''))
        self.imu_topic = str(bag_config.get('imu_topic', ''))
        self.gps_topic = str(bag_config.get('gps_topic', ''))

        self.name = self.path.stem

    def __repr__(self) -> str:
        return f"<ROSBag path={self.path}, topic={self.point_cloud_topic}, imu:{self.imu_topic}, gps: {self.gps_topic}>"

    def add_compressed_bag(self, compression_name: str, bag_path: Path) -> None:
        self.compressed_bags[compression_name] = bag_path

    def get_length(self) -> float | None:
        cmd = f"ros2 bag info {str(self.path)} | grep 'Duration:' | awk '{{print $2}}' | sed 's/s//'"
        try:
            duration = subprocess.check_output(cmd, shell=True, text=True).strip()

            return round(float(duration), 1)
        except subprocess.CalledProcessError as e:
            print("[ERROR] Checking bag length:", e)
            return None
    


class Compression:
    def __init__(self, config: Dict[str, Any], basedir: Path):
        self.dir = Path(basedir)
        self.name = config.get('name', '')
        self.pkg = config.get('launch_pkg') 
        self.launch_file = config.get('launch_file')
        self.launch_args_raw = config.get('launch_args', [])
        self.bag_rate = str(config.get('bag_rate', 1.0))
        self.post_delay = config.get('post_delay', 1.0)

    def get_launch_command(self, rosbag: ROSBag) -> List[str]:
        cmd = ['ros2', 'launch', self.pkg, self.launch_file]
        additional_topics = f"{rosbag.imu_topic} {rosbag.gps_topic}"
        dir = self.dir / 'compressed' / self.name
        args = [
            arg.format(
                bag_path = dir / rosbag.name,
                point_cloud_topic = rosbag.point_cloud_topic,
                stats_path = dir / 'stats',
                output_topic = "/decompressed",
                additional_topics = additional_topics
            )
            for arg in self.launch_args_raw
        ]
        cmd.extend(args)
        return cmd

    def launch(self, rosbag: ROSBag) -> None:
        bag_proc, launch_proc = None, None
        try:
            launch_cmd = self.get_launch_command(rosbag)
            launch_proc = subprocess.Popen(launch_cmd, 
                                           stdin=subprocess.DEVNULL, 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE,
                                           preexec_fn=os.setsid)
            if DEBUG:
                print(f"[DEBUG] Compression {self.name}: {launch_cmd}")

            bag_cmd = ['ros2', 'bag', 'play', rosbag.path, '--rate', self.bag_rate]
            if DEBUG:
                print(f"[DEBUG] Bag {self.name}: {launch_cmd}")

            bag_proc = subprocess.Popen(bag_cmd, 
                                        stdin=subprocess.DEVNULL, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE)

            print(f"[INFO] Launched rosbag and compression. Waiting for rosbag to finish...")
            bag_length = rosbag.get_length()
            if (bag_length := rosbag.get_length()):
                for _ in tqdm(range(int(bag_length*10)), desc="Processing bag", leave=False):
                    time.sleep(0.1)  # Simulate work

            bag_proc.wait()

            print(f"[INFO] Rosbag finished. Waiting {self.post_delay} seconds before shutting down launch.")
            time.sleep(self.post_delay)

            # Terminate the launch process group
            os.killpg(os.getpgid(launch_proc.pid), signal.SIGTERM)
            print("[INFO] Compression terminated.")

            rosbag.add_compressed_bag(self.name, self.dir / 'compressed' / self.name / rosbag.name)

        except KeyboardInterrupt:
            print("[INFO] Interrupted. Cleaning up...")
            for proc in (bag_proc, launch_proc):
                if proc and proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)

class Slam:
    def __init__(self, config: Dict[str, Any], dir: Path):
        """Initialize SLAM pipeline configuration"""
        self.dir = Path(dir)
        self.name = config.get('name')
        self.use_gps = config.get('gps', False)
        self.use_imu = config.get('imu', False)
        self.post_delay = config.get('post_delay', 1.0)

    def launch(self, rosbag : ROSBag, compression: Optional[Compression] = None) -> None:
        """Launch SLAM pipleine on the given rosbag."""
        lidar_topic = "/decompressed" if compression else rosbag.point_cloud_topic
        map_path = self.dir / 'maps' / f"{rosbag.name}_{compression.name if compression else ''}_{self.name}.simplemap"
        traj_path = self.dir / 'traj' / f"{rosbag.name}_{compression.name if compression else ''}_{self.name}.tum"
        bag_path = rosbag.compressed_bags[compression.name] if compression else rosbag.path
       
        # Create directories
        map_dir = self.dir / 'maps'
        map_dir.mkdir(parents=True, exist_ok=True)

        traj_dir = self.dir / 'traj'
        traj_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env.update({
            "MOLA_LIDAR_TOPIC": lidar_topic,
            "MOLA_GENERATE_SIMPLEMAP": "true",
            "MOLA_SIMPLEMAP_OUTPUT": str(map_path),
            "MOLA_SAVE_TRAJECTORY": "true",
            "MOLA_TUM_TRAJECTORY_OUTPUT": str(traj_path),
            "MOLA_USE_FIXED_LIDAR_POSE": "true"
        })

        if self.use_gps:
            env["MOLA_GPS_TOPIC"] = rosbag.gps_topic
            env["MOLA_USE_FIXED_GPS_POSE"] = "true"

        if self.use_imu:
            env["MOLA_IMU_TOPIC"] = rosbag.imu_topic
            env["MOLA_USE_FIXED_IMU_POSE"] = "true"
        
        slam_proc = None
        try:
            print(f"[INFO] Launching SLAM for {rosbag.name}, "
                  f"{compression.name if compression else 'None'}..."
            )

            slam_proc = subprocess.Popen(['mola-lo-gui-rosbag2', bag_path], 
                                           stdin=subprocess.DEVNULL, 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE,
                                           env=env)


            slam_proc.wait()

            print(f"[INFO] SLAM complete. Waiting {self.post_delay}s.")
            time.sleep(self.post_delay)
             
        except KeyboardInterrupt:
            print("[INFO] Interrupted. Cleaning up...")
            if slam_proc and slam_proc.poll() is None:
                os.killpg(os.getpgid(slam_proc.pid), signal.SIGTERM)

def load_benchmark_process_config(constructor: Callable[..., Any], config: List[Dict[str, Any]], *args: Any) -> List[Any]:
    """Load list of process configs by applying a constructo to each config entry."""
    return [constructor(cfg, *args) for cfg in config]

def run_compression(config: Dict[str, Any], benchmark_dir: Path, cache: Optional[Dict[str, Any]]) -> Tuple[List[ROSBag], List[Compression], Path]:
    """Run compression pipeline or load previous result from pickle if available."""
    benchmark_dir = Path(benchmark_dir)

    if cache:
        return cache['bags'], cache['compression_launches'], cache['benchmark_dir']

    bags = load_benchmark_process_config(ROSBag, config.get('bags', {}), benchmark_dir)
    compression_launches = load_benchmark_process_config(Compression, config.get('compression', {}), benchmark_dir)

    for bag in bags:
        print(f"[INFO] Compressing PC in bag: {bag.name}")
        for compression in compression_launches:
            print(f"[INFO] Running Compression: {compression.name}")
            compression.launch(bag)

        pickle_path = benchmark_dir / "compression_cache.pkl"
        with open(pickle_path, 'wb') as f:
            pickle.dump({
                'bags': bags, 
                'compression_launches': compression_launches, 
                'benchmark_dir': benchmark_dir
            }, f)

    return bags, compression_launches, benchmark_dir

def run_slam(config: Dict[str, Any], benchmark_dir: Path, bags: List[ROSBag], compression_launches: List[Compression]) -> List[Slam]:
    """Run SLAM benchmark for each combination of bag, compression, and SLAM config."""
    slam_launches = load_benchmark_process_config(Slam, config.get('slam', {}), benchmark_dir)

    print(f"[INFO] Starting SLAM Benchmarks...")
    total_iter = len(bags) * len(compression_launches) * len(slam_launches)

    with tqdm(total=total_iter) as pbar:
        for bag in bags:
            for compression in compression_launches:
                for slam in slam_launches:
                    slam.launch(bag, compression)
                    pbar.update(1)

    return slam_launches

def run_benchmark(config_path: Path, compression_cache: Optional[Dict[str, Any]]) -> None:
    """Top-level entry point to run the full benchmark pipeline."""
    benchmark_dir = Path(datetime.now().strftime("benchmark_%Y-%m-%d_%H-%M-%S"))
    with open(config_path, 'r') as f:
        config = json.load(f)

    bags, compression_launches, benchmark_dir = run_compression(config, benchmark_dir, compression_cache)

    run_slam(config, benchmark_dir, bags, compression_launches)

def load_pickle(path: Path) -> Any:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"No file found at {path}")

    with open(path, 'rb') as f:
        return pickle.load(f)

def main() -> None:
    parser = argparse.ArgumentParser(description="Run rosbag + launch file benchmarks from a config JSON.")
    parser.add_argument('--config', required=True, help='Path to JSON benchmark config file')
    parser.add_argument('--load_compress', type=str, default="", help='Path to previously saved compression pickle')
    args = parser.parse_args()
    
    compression_data = None
    if args.load_compress:
        try:
            compression_data = load_pickle(args.load_compress)
            print("[INFO] Loaded compression stage from pickle.")
        except Exception as e:
            print("[ERROR] Failed to load compression data:", e)

    run_benchmark(args.config, compression_data)

if __name__ == '__main__':
    main()
