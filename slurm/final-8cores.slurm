#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --output=output-144G.txt
#SBATCH --error=error-144G.txt

module purge
module load intel-compilers/2022.1.0
module load GCCcore/11.3.0
module load GCC/11.3.0
module load Python/3.10.4
module load OpenMPI/4.1.4
module load mpi4py/3.1.4

srun -n 8 python3 main.py 144g