#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task 1
#SBATCH --output=../output/testRun.txt
#SBATCH --error=../output/testRun.txt
module purge
module load Python/3.8.19
mpirun python -u ../main.py --datasize 106k