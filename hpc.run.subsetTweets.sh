#! /bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -N hpc.run.subsetTweets.09232017-10192017
#$ -e hpc.run.subsetTweets.09232017-10192017.error
#$ -o hpc.run.subsetTweets.09232017-10192017.out
#$ -l h_vmem=60G

./run.subsetTweets.sh
