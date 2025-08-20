#!/usr/bin/python3
import sys
import re
import argparse
from pathlib import Path
from rich import print

class Data:
    def __init__(self, dir: Path):
        self.RCPCC_PATTERN = re.compile(r"(rcpcc_\d+)")
        self.MULRAN_PATTERN = re.compile(r"(Riverside|DCC|KAIST|Sejong 0\d)")
        
        self.dir = dir
        self.find_tum_files()
        self.split_tum_files()

    def find_tum_files(self):
        self.paths = [str(path.resolve()) for path in self.dir.rglob("*tum*")] if self.dir.is_dir() else []
    
    def split_tum_files(self):
        # split by compression
        by_compression = {}
        for path in self.paths:
            if (match := self.RCPCC_PATTERN.search(path)):
                key = match.group(1)
                by_compression[key] = by_compression.get(key, []) + [path]
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
        "--directory",
        type=Path,
        default="./",
        help="Path to the directory to search"
    )

    args = parser.parse_args()
    
    # Find TUM files
    data = Data(args.directory)
    print(data)
        

if __name__ == "__main__":
    main()
