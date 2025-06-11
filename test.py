from tqdm import tqdm
import time

outer_iterations = 5
inner_iterations = 10

for i in tqdm(range(outer_iterations), desc="Outer loop"):
    for j in tqdm(range(inner_iterations), desc="Inner loop", leave=False):
        time.sleep(0.1)  # Simulate work

