This repository has code with a quick test of the most efficient settings for `samtools sort` memory per thread and CPUs.

The results were generated to answer a question at https://bioinformatics.stackexchange.com/questions/18538/samtools-sort-most-efficient-memory-and-thread-settings-for-many-samples-on-a-c

The interactive plots mentioned in my stackexchange post (in the form of an .html page) can be
downloaded from:
1. Plots from original post (looking at duration): `SAMTOOLS_SORT/samtools_sort_CPU_and_memory_comparison.tar.gz`
2. Plots from update 2 (comparing `samtools + zlib`, `samtools + libdeflate`, and `sambamba`): `SAMTOOLS_SORT/samtools_with_zlib_or_libdeflate_vs_sambamba.tar.gz`
