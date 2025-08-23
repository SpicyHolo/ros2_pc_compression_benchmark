import matplotlib.pyplot as plt
import numpy as np

from datetime import datetime
from pathlib import Path
from typing import Tuple

import evo.tools.plot as plot
import evo.core.lie_algebra as lie

from evo.core import metrics, sync
from evo.tools import file_interface as fi
from evo.core.trajectory import PoseTrajectory3D

DEBUG = False

def calculate_metrics(traj_path : Path, ref_path : Path, use_RPE : bool = False, save_plot_path: str = "") -> Tuple[float, float]:
    # Load TUM trajectories
    traj = fi.read_tum_trajectory_file(traj_path)
    ref_traj = fi.read_tum_trajectory_file(ref_path)

    if DEBUG:
        print(f"Reading trajectory: {traj_path}")   
        print(traj)
        print(f"Reading reference: {ref_path}")   
        print(ref_traj)

    if DEBUG:
        dt1, dt2 = datetime.fromtimestamp(ref_traj.timestamps[0]), datetime.fromtimestamp(traj.timestamps[0])
        delta = dt2 - dt1
        print("Aligning timestamps, assuming first timestamps are aligned.")
        print(f"Trajectory: {dt2}, Reference: {dt1}, Difference: {delta}")

    # Align timestamps
    offset = abs(ref_traj.timestamps[0] - traj.timestamps[0])

    # Match to nearest measurement, if they are not  
    ref_traj, traj = sync.associate_trajectories(ref_traj, traj, offset_2=-offset)
    
    # Align origins
    ref_traj.align_origin(traj)

    # Somehow the end up being rotated by 180 degrees
    T_z180 = lie.se3(np.array([[-1, 0, 0], [0, -1, 0], [0, 0, 1]]), np.zeros(3))
    traj.transform(T_z180)

    if use_RPE:
        # Translation error
        metric = metrics.RPE(all_pairs=True, delta = 100, delta_unit = metrics.Unit.meters)
        metric.process_data((ref_traj, traj))

        stats = metric.get_all_statistics()
        
        if DEBUG:
            print("rRPE, translation(m):")
            print(stats)

        # Percentage error over whole trajecotry
        t_err = stats["mean"] 


        metric = metrics.RPE(metrics.PoseRelation.rotation_angle_deg, all_pairs=True, delta=10, delta_unit= metrics.Unit.meters)
        metric.process_data((ref_traj, traj))
        stats = metric.get_all_statistics()

        if DEBUG:
            print("RPE, rotation(deg):")
            print(stats)

        r_err = stats["mean"]

    else:
        # Translation error
        metric = metrics.APE()
        metric.process_data((ref_traj, traj))

        stats = metric.get_all_statistics()
        
        if DEBUG:
            print("APE, translation(m):")
            print(stats)

        # Percentage error over whole trajecotry
        t_err = stats["mean"] 


        metric = metrics.APE(metrics.PoseRelation.rotation_angle_deg)
        metric.process_data((ref_traj, traj))
        stats = metric.get_all_statistics()

        if DEBUG:
            print("APE, rotation(deg):")
            print(stats)

        r_err = stats["mean"]
        
    if save_plot_path:
        fig_traj = plt.figure()
        ax_traj = plot.prepare_axis(fig_traj, plot.PlotMode.xy)
        plot.traj(ax_traj, plot.PlotMode.xy,ref_traj, label='Reference', color="red", style="--")
        plot.traj(ax_traj, plot.PlotMode.xy, traj, label='Trajectory', color="black")
        ax_traj.set_title("Trajectory Comparison")
        ax_traj.set_xlabel("X [m]")
        ax_traj.set_ylabel("Y [m]")
        ax_traj.legend()
        plt.savefig(save_plot_path)
        plt.close()

    return t_err, r_err


# Paths to your TUM trajectories
if __name__ == "__main__":
    DATASET = "DCC03"
    COMPRESSION = "raw"
    SLAM = "kiss"

    traj_file = f"/workspace/datasets/benchmark_2025-08-22_23-07-55/{SLAM}/{DATASET}/{COMPRESSION}/{DATASET}_poses_tum.txt"        # your trajectory
    ref_file = f"/workspace/datasets/mulran_gt/{DATASET}.tum"     # reference trajectory

    t_err, r_err = calculate_metrics(Path(traj_file), Path(ref_file))
    print(f"Transl[%]: {t_err}, Rotation[deg/m]: {r_err}")
    

