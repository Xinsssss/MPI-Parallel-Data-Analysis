#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --output=output.txt
#SBATCH --error=error.txt

module purge
module load intel-compilers/2022.1.0
module load GCCcore/11.3.0
module load GCC/11.3.0
module load Python/3.10.4
module load OpenMPI/4.1.4
module load mpi4py/3.1.4

srun -n 8 python3 test.py