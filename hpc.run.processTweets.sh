#! /bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -N hpc.run.processTweets.[date_range]
#$ -e hpc.run.processTweets.[date_range].error
#$ -o hpc.run.processTweets.[date_range].out
#$ -pe mpi 4

./run.processTweets.sh 4
