for d in /workspace/datasets/MulRan/*/; do
    dirname=$(basename "$d")
    echo $dirname
    python gt.py "$d" --topic /gt --output "${dirname}.tum"
done
