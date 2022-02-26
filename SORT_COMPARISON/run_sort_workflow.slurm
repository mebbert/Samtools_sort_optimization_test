#!/bin/bash
#SBATCH --time=72:15:00             				# Time limit for the job (REQUIRED).
#SBATCH --job-name=NF_Parent_Sort				    # Job name
#SBATCH --ntasks=1                  				# Number of cores for the job. Same as SBATCH -n 1
#SBATCH --mem=5G                    				# Total memory requested
#SBATCH --partition=normal          				# Partition/queue to run the job in. (REQUIRED)
#SBATCH -e slurm/slurm-%j.err             				# Error file for this job.
#SBATCH -o slurm/slurm-%j.out             				# Output file for this job.
#SBATCH -A coa_mteb223_uksr 	    				# Project allocation account name (REQUIRED)

export NXF_WORK=/mnt/gpfs3_amd/condo/mteb223/mteb223/Samtools_sort_optimization_test/SORT_COMPARISON/work

module load ccs/java/jdk1.8.0_202
nextflow run SORT_COMPARISON.nf \
	-with-report all_three-report-queue_size_30.html \
	-with-trace all_three-trace-queue_size_30.txt \
	-with-timeline
