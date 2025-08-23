#!/usr/bin/python3
import sys
import re
import argparse
from pathlib import Path
from rich import print

from calculate_metrics import calculate_metrics

import numpy as np
import pandas as pd

DEBUG = False
class Data:
    def __init__(self, dir: Path, ref_dir : Path):
        self.RCPCC_PATTERN = re.compile(r"(rcpcc_\d+)")
        self.MULRAN_PATTERN = re.compile(r"(Riverside0\d|DCC0\d|KAIST0\d|Sejong0\d)")
        
        self.dir = dir
        self.ref_dir = ref_dir
        self.find_ref_tum()
        self.find_tum_files()
        self.split_tum_files()
        
        self.df = pd.DataFrame({
            "compression_name": pd.Series(dtype="str"),
            "dataset_name": pd.Series(dtype="str"),
            "t_err": pd.Series(dtype="float"),
            "r_err": pd.Series(dtype="float")
        }) 
        
        for compression, v in self.dataset.items():
            for mulran, paths in v.items():
                print(f"{compression}, {mulran}")
                for path in paths:
                    ref = self.references[mulran]

                    t_err, r_err = calculate_metrics(path, ref, use_RPE=False, save_plot_path=f"./res/{mulran}_{compression}")
                    self.df.loc[len(self.df)] = [compression, mulran, t_err, r_err]

        print(self.df)
        self.df.to_csv("./res/test.csv", sep='\t', header=True) 


    def find_ref_tum(self):
        paths = [str(path.resolve()) for path in self.ref_dir.rglob("*tum*")] if self.dir.is_dir() else []
        self.references = {}

        for path in paths:
            if (match := self.MULRAN_PATTERN.search(path)):
                key = match.group(1)
                self.references[key] = str(path) 

    def find_tum_files(self):
        self.paths = [str(path.resolve()) for path in self.dir.rglob("*tum*")] if self.dir.is_dir() else []
    
    def split_tum_files(self):
        # split by compression
        by_compression = {}
        for path in self.paths:
            if (match := self.RCPCC_PATTERN.search(path)):
                key = match.group(1)
                by_compression[key] = by_compression.get(key, []) + [path]
            elif "raw" in path:
                by_compression["raw"] = by_compression.get("raw", []) + [path]
            else:
                print(f"[red][ERROR] Path doesn't contain compression signature: {path}")
                sys.exit(1)
        
        
        # Split by mulran datasets
        self.dataset = {}
        for compression, paths in by_compression.items():
            self.dataset[compression] = {}
            for path in paths:
                if (match := self.MULRAN_PATTERN.search(path)):
                    mulran_name = match.group(1)
                    self.dataset[compression][mulran_name] = self.dataset[compression].get(mulran_name, []) + [path]               

    def __str__(self):
        res = ""
        for compression, v in self.dataset.items():
            res += f"{compression}:\n"
            for mulran, paths in v.items():
                res += f"\t{mulran}:\n"
                for path in paths:
                    res += f"\t\t{path}\n"
        return res

def main():
    parser = argparse.ArgumentParser(
        description="Find all .tum files in a directory and print their absolute paths."
    )
    parser.add_argument(
        "-d",
        "--data",
        type=Path,
        default="./",
        help="Path to the directory to search"
    )
    parser.add_argument(
        "-r",
        "--reference",
        type=Path,
        default="./",
        help="Path to the directory to search for references"
    )

    args = parser.parse_args()
    
    # Find TUM files
    data = Data(args.data, args.reference)
        

if __name__ == "__main__":
    main()
