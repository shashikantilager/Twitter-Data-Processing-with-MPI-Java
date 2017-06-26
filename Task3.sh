#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=4
module load Java/1.8.0_71
module load mpj/0.44
javac -cp .:$MPJ_HOME/lib/mpj.jar:json-simple-1.1.jar MPITwitterAnalysis.java
mpjrun.sh -cp  .:json-simple-1.1.jar -np 8  MPITwitterAnalysis bigTwitter.json 
