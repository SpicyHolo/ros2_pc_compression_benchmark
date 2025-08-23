#!/usr/bin/python3

import argparse
import json
import os
import pickle
import signal
import subprocess
import time
import shutil
import sys

from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Callable
from rich import print

DEBUG = True


class ROSBag:
    def __init__(self, bag_config: Dict[str, Any], basedir : Path):
        self.compressed_bags: Dict[str, Path] = {}

        if DEBUG:
            print(f"[magenta][DEBUG] Created ROSBag object.")
            print(f"[magenta]{bag_config}")

        self.dir = Path(basedir)
        self.path = Path(bag_config.get("path", ""))
        self.read_ahead_queue_size = str(bag_config.get("read_ahead_queue_size", 1000))

        self.point_cloud_topic = bag_config.get("point_cloud_topic", "")
        self.imu_topic = bag_config.get("imu_topic", "")
        self.gps_topic = bag_config.get("gps_topic", "")

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
            print("[red][ERROR] Checking bag length:", e)
            return None
    
# //////////////////////////////////////////
# COMPRESSION
class Compression:
    def __init__(self, config: Dict[str, Any], basedir: Path):
        self.dir = Path(basedir)
        self.name = config.get("name", "")
        self.pkg = config.get("launch_pkg") 
        self.launch_file = config.get("launch_file")
        self.launch_args_raw = config.get("launch_args", [])
        self.bag_rate = str(config.get("bag_rate", 1.0))
        self.post_delay = config.get("post_delay", 1.0)
        self.compressed_topic = config.get("compressed_topic", "/decompressed")

    def get_launch_command(self, rosbag: ROSBag) -> List[str]:
        cmd = ["ros2", "launch", self.pkg, self.launch_file]
        additional_topics = f"{rosbag.imu_topic} {rosbag.gps_topic}"
        dir = self.dir / "compressed" / self.name
        args = [
            arg.format(
                bag_path = dir / rosbag.name / rosbag.name,
                point_cloud_topic = rosbag.point_cloud_topic,
                stats_path = dir / rosbag.name / 'stats',
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
                                           stdout=subprocess.DEVNULL,
                                           preexec_fn=os.setsid)
            if DEBUG:
                print(f"[magenta][DEBUG] Launching compression {self.name}:")
                print(f"[magenta]{launch_cmd}")

            bag_cmd = ['ros2', 'bag', 'play', rosbag.path, '--rate', self.bag_rate, '--disable-keyboard-controls']

            if DEBUG:
                print(f"[magenta][DEBUG] Starting bag {self.name}:")
                print(f"[magenta]{bag_cmd}")

            bag_proc = subprocess.Popen(bag_cmd, 
                                        stdin=subprocess.DEVNULL,
                                        stdout=subprocess.DEVNULL,
                                        preexec_fn=os.setsid)

            print(f"[green][INFO] Launched rosbag and compression. Waiting for rosbag to finish...")
            if (bag_length := rosbag.get_length()):
                print(f"[green][INFO] Estimated length: {bag_length / float(self.bag_rate):.2f}s")  

            bag_proc.wait()

            print(f"[green][INFO] Rosbag finished. Waiting {self.post_delay} seconds before shutting down launch.")
            time.sleep(self.post_delay)

            # Terminate the launch process group
            os.killpg(os.getpgid(launch_proc.pid), signal.SIGINT)
            print("[green][INFO] Compression terminated.")

            rosbag.add_compressed_bag(self.name, self.dir / 'compressed' / self.name / rosbag.name / rosbag.name)

        except KeyboardInterrupt:
            print("[green][INFO] Interrupted. Cleaning up...")
            for proc in (bag_proc, launch_proc):
                if proc and proc.poll() is None:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
                    except ProcessLookupError:
                        pass
            sys.exit(1)

# //////////////////////////////////////////
# SLAM
class BaseSLAM:
    def __init__(self, config: Dict[str, Any], dir: Path):
        self.dir = Path(dir)
        self.name = config.get("name")
        self.post_delay = config.get("post_delay", 1.0)

    def launch(self, rosbag, compression: Optional[Any] = None) -> None:
        """Launch the SLAM pipeline. Child classes must implement build_command and prepare_env."""
        slam_cmd = self.build_command(rosbag, compression)
        env = self.prepare_env(rosbag, compression)

        slam_proc = None
        try:
            print(f"[green][INFO] Launching SLAM ({self.name}) for {rosbag.name} {compression.name if compression else ''}...")

            if DEBUG:
                print(f"[magenta]{slam_cmd}")

            slam_proc = subprocess.Popen(slam_cmd, env=env)
            slam_proc.wait()

            print(f"[green][INFO] SLAM complete. Waiting {self.post_delay}s.")
            time.sleep(self.post_delay)

        except KeyboardInterrupt:
            print("[green][INFO] Interrupted. Cleaning up...")
            if slam_proc and slam_proc.poll() is None:
                os.killpg(os.getpgid(slam_proc.pid), signal.SIGINT)

    def build_command(self, rosbag, compression: Optional[Any]):
        """Child classes must implement this method to build the SLAM command."""
        raise NotImplementedError

    def prepare_env(self, rosbag, compression: Optional[Any]):
        """Child classes can override to provide environment variables."""
        return os.environ.copy()

class KissSlam(BaseSLAM):
    def __init__(self, config: Dict[str, Any], dir: Path):
        super().__init__(config, dir)
        self.visualize =  config.get('visualize', False)
        self.config = config

    def build_command(self, rosbag, compression: Optional[Any]):
        lidar_topic = compression.compressed_topic if compression else rosbag.point_cloud_topic
        bag_path = rosbag.compressed_bags[compression.name] if compression else rosbag.path
        config_path = self.config.get('config', "")

        cmd = ["kiss_slam_pipeline", "--dataloader", "rosbag", "--topic", lidar_topic]
        if config_path:
            cmd.extend(["--config", config_path])

        if self.visualize:
            cmd.append("--visualize")
            
        cmd.append(bag_path)
        return cmd

    
    # Override launch(), to add copy commands at the end
    def launch(self, rosbag, compression: Optional[Any] = None):
        slam_cmd = self.build_command(rosbag, compression)
        slam_proc = None
        try:
            print(f"[green][INFO] Launching SLAM ({self.name}) for {rosbag.name}...")
            if DEBUG:
                print(f"[magenta]{slam_cmd}")
            slam_proc = subprocess.Popen(slam_cmd)
            slam_proc.wait()
            print(f"[green][INFO] SLAM complete. Copying results...")

            # Example copy commands
            (self.dir / "kiss" / rosbag.name).mkdir(parents=True, exist_ok=True)

            if compression:
                shutil.copytree(Path("slam_output") / "latest", self.dir / "kiss" / rosbag.name / compression.name, dirs_exist_ok=True)
            else:
                shutil.copytree(Path("slam_output") / "latest", self.dir / "kiss" / rosbag.name / "raw", dirs_exist_ok=True)

            print(f"[green][INFO] Copy complete. Waiting {self.post_delay}s.")
            time.sleep(self.post_delay)

        except KeyboardInterrupt:
            print("[green][INFO] Interrupted. Cleaning up...")
            if slam_proc and slam_proc.poll() is None:
                os.killpg(os.getpgid(slam_proc.pid), signal.SIGTERM)

class MolaOdometry(BaseSLAM):
    def __init__(self, config: Dict[str, Any], dir: Path):
        super().__init__(config, dir)
        self.use_gps = config.get('gps', False)
        self.use_imu = config.get('imu', False)

    def prepare_env(self, rosbag, compression: Optional[Any]):
        env = os.environ.copy()
        env.update({"MOLA_USE_FIXED_LIDAR_POSE": "true"})

        if self.use_gps:
            env["MOLA_GPS_TOPIC"] = rosbag.gps_topic
            env["MOLA_USE_FIXED_GPS_POSE"] = "true"

        if self.use_imu:
            env["MOLA_IMU_TOPIC"] = rosbag.imu_topic
            env["MOLA_USE_FIXED_IMU_POSE"] = "true"
        return env 

    def build_command(self, rosbag, compression: Optional[Any]):
        lidar_topic = compression.compressed_topic if compression else rosbag.point_cloud_topic
        map_path = self.dir / "maps" / f"{rosbag.name}_{compression.name if compression else ''}_{self.name}.simplemap"
        traj_path = self.dir / "traj" / f"{rosbag.name}_{compression.name if compression else ''}_{self.name}.tum"
        bag_path = rosbag.compressed_bags[compression.name] if compression else rosbag.path

        # Ensure directories exist
        (self.dir / "maps").mkdir(parents=True, exist_ok=True)
        (self.dir / "traj").mkdir(parents=True, exist_ok=True)
        
        ros2_prefix = subprocess.check_output(["ros2", "pkg", "prefix", "mola_lidar_odometry"], text=True).strip()

        cmd = ["mola-lidar-odometry-cli",
                    f"-c {ros2_prefix}/share/mola_lidar_odometry/pipelines/lidar3d-default.yaml",
                    f"--input-rosbag2 {bag_path}",
                    f"--lidar-sensor-label {lidar_topic}",
                    f"--output-tum-path {str(traj_path)}",
                    f"--output-simplemap {str(map_path)}"
        ]
        return cmd

# //////////////////////////////////////////
def load_benchmark_process_config(constructor: Callable[..., Any], config: List[Dict[str, Any]], *args: Any) -> List[Any]:
    """Load list of process configs by applying a constructor to each config entry."""
    return [constructor(cfg, *args) for cfg in config]

def slam_factory_fn(config: Dict[str, Any], dir: Path):
    """ Factory function for BaseSLAM child classes """
    name = config.get('name', '')

    if name == 'mola':
        return MolaOdometry(config, dir)
    elif name == 'kiss':
        return KissSlam(config, dir)
    else:
        print(f"[red][ERROR] Unknown SLAM name: {name}")
        print(config.get('slam', {}))
        sys.exit(1)

def run_compression(config: Dict[str, Any], benchmark_dir: Path, cache: Optional[Dict[str, Any]]) -> Tuple[List[ROSBag], List[Compression], Path]:
    """Run compression pipeline or load previous result from pickle if available."""
    benchmark_dir = Path(benchmark_dir)

    if cache:
        return cache['bags'], cache['compression_launches'], cache['benchmark_dir']

    bags = load_benchmark_process_config(ROSBag, config.get('bags', {}), benchmark_dir)
    compression_launches = load_benchmark_process_config(Compression, config.get('compression', {}), benchmark_dir)

    total_bags = len(bags) * len(compression_launches)

    i = 1
    for bag in bags:
        for compression in compression_launches:
            print(f"[cyan]\nRunning Compression {compression.name} for {bag.name} ({i}/{total_bags})")
            compression.launch(bag)
            i += 1

    (benchmark_dir).mkdir(parents=True, exist_ok=True)
    pickle_path = benchmark_dir / "compression_cache.pkl"
    with open(pickle_path, 'wb') as f:
        pickle.dump({
            'bags': bags, 
            'compression_launches': compression_launches, 
            'benchmark_dir': benchmark_dir
        }, f)

    return bags, compression_launches, benchmark_dir

def run_slam(config: Dict[str, Any], benchmark_dir: Path, bags: List[ROSBag], compression_launches: List[Compression]) -> List[MolaOdometry | KissSlam]:
    """Run SLAM benchmark for each combination of bag, compression, and SLAM config."""
    slam_launches = load_benchmark_process_config(slam_factory_fn, config.get('slam', {}), benchmark_dir)

    total_bags = len(bags) * len(compression_launches) * len(slam_launches) 
    total_bags += len(bags) * len(slam_launches)
    i = 1
    for bag in bags:
        # no compression
        for slam in slam_launches:
            print(f"[cyan]\nRunning SLAM {slam.name} for {bag.name} ({i}/{total_bags})")
            slam.launch(bag)
            i += 1

        # for each compression
        for compression in compression_launches:
            for slam in slam_launches:
                print(f"[cyan]\nRunning SLAM {slam.name} for {bag.name} {compression.name} ({i}/{total_bags})")
                slam.launch(bag, compression)
                i += 1

    return slam_launches

def run_benchmark(config_path: Path, result_dir: Optional[Path], compression_cache: Optional[Dict[str, Any]]) -> None:
    """Top-level entry point to run the full benchmark pipeline."""
    if result_dir:
        benchmark_dir = Path(result_dir) 
    benchmark_dir = Path.cwd() / Path(datetime.now().strftime("benchmark_%Y-%m-%d_%H-%M-%S"))
    with open(config_path, 'r') as f:
        config = json.load(f)

    print()
    print("[cyan]STAGE 1: COMPRESSION")
    print("[cyan]####################")
    bags, compression_launches, benchmark_dir = run_compression(config, benchmark_dir, compression_cache)

    print()
    print("[cyan]STAGE 2: SLAM")
    print("[cyan]####################")
    run_slam(config, benchmark_dir, bags, compression_launches)

def load_pickle(path: Path) -> Any:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"[red][ERROR] No file found at {path}")

    with open(path, 'rb') as f:
        return pickle.load(f)

def main() -> None:
    parser = argparse.ArgumentParser(description="Run rosbag + launch file benchmarks from a config JSON.")
    parser.add_argument('-c', '--config',           type=str, default="", help='Path to JSON benchmark config file')
    parser.add_argument('-l', '--load-compression', type=str, default="",                        help='Path to previously saved compression pickle')
    parser.add_argument('-d', '--result-dir',       type=str, default="",                        help='Path where to store results, defaults to cwd')
    args = parser.parse_args()

    # Load stored compressions, if provided
    compression_data = None
    if args.load_compression:
        try:
            compression_data = load_pickle(args.load_compression)
            print("[green][INFO] Loaded compression stage from pickle.")
        except Exception as e:
            print("[red][ERROR] Failed to load compression data:", e)

    config_path = Path(args.config) if args.config else Path(__file__).parent / "config.json"
    result_path = Path(args.result_dir) if args.result_dir else Path.cwd()

    run_benchmark(config_path, result_path, compression_data)

if __name__ == '__main__':
    main()
