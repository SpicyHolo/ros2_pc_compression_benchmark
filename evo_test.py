import copy
import numpy as np

import evo.core.trajectory as et
import evo.core.trajectory as et
import evo.core.lie_algebra as lie

from evo.core import metrics, sync
from evo.tools import file_interface as fi
from evo.core.metrics import APE

import evo.tools.plot as plot
import matplotlib.pyplot as plt

# Paths to your TUM trajectories
traj_file = "/workspace/datasets/benchmark_2025-08-18_23-06-29/kiss/Riverside03/raw/Riverside03_poses_tum.txt"        # your trajectory
ref_file = "/workspace/datasets/mulran_gt/Riverside03.tum"     # reference trajectory

# Load TUM trajectories
traj = fi.read_tum_trajectory_file(traj_file)
ref_traj = fi.read_tum_trajectory_file(ref_file)

# Align timestamps
traj.timestamps -= traj.timestamps[0]
ref_traj.timestamps -= ref_traj.timestamps[0]

# Match to nearest measurement, if they are not  
ref_traj, traj = sync.associate_trajectories(ref_traj, traj)

# Align origins
ref_traj.align_origin(traj)  # aligns first pose translation + rotation

# Somehow the end up being rotated by 180 degrees
T_z180 = lie.se3(np.array([[-1, 0, 0], [0, -1, 0], [0, 0, 1]]), np.zeros(3))
traj.transform(T_z180)

# Compute Absolute Pose Error
pose_relation = metrics.PoseRelation.translation_part
ape_metric = APE(pose_relation)
data = (ref_traj, traj)
ape_metric.process_data(data)

rms_error = ape_metric.get_statistic(metrics.StatisticsType.rmse)
total_distance = ref_traj.path_length
percent_error = (rms_error / total_distance) * 100

print(f"APE RMSE: {rms_error:.4f} m")
print(f"General trajectory % error: {percent_error:.2f}%")

# Plot trajectories in 3D
fig_traj = plt.figure()
ax_traj = plot.prepare_axis(fig_traj, plot.PlotMode.xy)
plot.traj(ax_traj, plot.PlotMode.xy,ref_traj, label='Reference', color="red", style="--")
plot.traj(ax_traj, plot.PlotMode.xy, traj, label='Trajectory', color="black")
ax_traj.set_title("Trajectory Comparison")
ax_traj.set_xlabel("X [m]")
ax_traj.set_ylabel("Y [m]")
ax_traj.legend()
plt.show()
