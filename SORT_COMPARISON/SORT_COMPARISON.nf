
/*
 * Make this pipeline a nextflow 2 implementation
 */
nextflow.enable.dsl=2

input_files = [
               file('../REALIGN_SAMPLE/A-CUHS-CU003128-BL-COL-49696BL1.unsorted.bam'),
               // file('../COLLATE_SAMPLES/A-CUHS-CU000208-BL-COL-56227BL1_vcpa1.0_GRU-IRB-PUB.collated.bam'),
               file('../COLLATE_SAMPLES/A-CUHS-CU002707-BL-COL-40848BL1_vcpa1.0_GRU-IRB-PUB.collated.bam')
              ]


workflow {

    all_mem_and_cpu_ch = generate_mem_and_cpu_tuples()

    /*
     * Combine input files with mem & CPU tuples
     */
    Channel.from(input_files)
        | combine( all_mem_and_cpu_ch )
        // | view()
        | set { input_file_ch }

    test_input = Channel.of( [file('../REALIGN_SAMPLE/A-CUHS-CU003128-BL-COL-49696BL1.unsorted.bam'), 5, 3] )
                    .view()
//    samtools_csort_proc( test_input )
//    samtools_libdeflate_csort_proc( test_input )
//    sambamba_csort_proc( test_input )

    samtools_csort_proc( input_file_ch )
    Lsamtools_csort_proc( input_file_ch )
    sambamba_csort_proc( input_file_ch )
}


process samtools_csort_proc {

    tag { "${bam.baseName};CPUs:${total_cpus};MEM_PER_THREAD:${mem_per_thread}GB" }

    executor='slurm'
    queue = 'normal'
    cpus { total_cpus }


    /*
     * In my experience, samtools always exceeds its allotted memory. Creating
     * a 20% buffer for most jobs. A 200% buffer for smaller jobs.
     */
    memory { if(total_cpus < 5 || mem_per_thread < 5){
                total_cpus * mem_per_thread.GB * 2
             }
             else{
                total_cpus * mem_per_thread.GB * 1.2
            } }
    clusterOptions = "--time 5:00:00 --account coa_mteb223_uksr"

    /*
     * Carry on if an individual instance fails.
     */
    errorStrategy 'ignore'
    
    /* delete files upon completion (I think) */
    /* This didn't work. */
    // cleanup = true

    afterScript 'rm *.bam*'


    input:
    tuple path(bam), val(total_cpus), val(mem_per_thread)

    script:

    additional_threads = task.cpus - 1


    """
    echo ""
    echo "n CPUs: $total_cpus"
    echo "Total mem: $task.memory"
    echo "mem_per_thread: $mem_per_thread"
    echo ""


    samtools sort \\
        -@ "${additional_threads}" \\
        -m ${mem_per_thread}G \\
        -o "${bam.baseName}.csorted.bam" \\
        -O bam \\
        -T "${bam.baseName}.csorted" \\
        --write-index \\
        "${bam}"
    """
}


/*
 * Lsamtools for libdeflate samtools
 */
process Lsamtools_csort_proc {

    tag { "${bam.baseName};CPUs:${total_cpus};MEM_PER_THREAD:${mem_per_thread}GB" }

    executor='slurm'
    queue = 'normal'
    cpus { total_cpus }


    /*
     * In my experience, samtools always exceeds its allotted memory. Creating
     * a 20% buffer for most jobs. A 200% buffer for smaller jobs.
     */
    memory {  if(total_cpus < 5 || mem_per_thread < 5){
                total_cpus * mem_per_thread.GB * 2
             }
             else{
                total_cpus * mem_per_thread.GB * 1.2
            }
           }
    clusterOptions = "--time 5:00:00 --account coa_mteb223_uksr"

    /*
     * Carry on if an individual instance fails.
     */
    errorStrategy 'ignore'
    
    /* delete files upon completion (I think) */
    /* This didn't work. */
    // cleanup = true

    afterScript 'rm *.bam*'


    input:
    tuple path(bam), val(total_cpus), val(mem_per_thread)

    script:

    additional_threads = task.cpus - 1


    """

    echo ""
    echo "n CPUs: $total_cpus"
    echo "Total mem: $task.memory"
    echo "mem_per_thread: $mem_per_thread"
    echo ""

    singularity run \\
    --app samtools115 ${projectDir}/samtools-1.15.sinf \\
    samtools sort \\
        -@ "${additional_threads}" \\
        -m ${mem_per_thread}G \\
        -o "${bam.baseName}.csorted.bam" \\
        -O bam \\
        -T "${bam.baseName}.csorted" \\
        --write-index \\
        "${bam}"
    """
}

process sambamba_csort_proc {

    tag { "${bam.baseName};CPUs:${total_cpus};MEM_PER_THREAD:${mem_per_thread}GB" }

    executor='slurm'
    queue = 'normal'
    cpus { total_cpus }

    /*
     * In my experience, sambamba always exceeds its allotted memory. Creating
     * a 20% buffer for most jobs. A 200% buffer for smaller jobs.
     */
    memory { if(total_cpus < 5 || mem_per_thread < 5){
                total_cpus * mem_per_thread.GB * 2
             }
             else{
                total_cpus * mem_per_thread.GB * 1.2
            }
           }

    clusterOptions = "--time 5:00:00 --account coa_mteb223_uksr"

    /*
     * Carry on if an individual instance fails.
     */
    errorStrategy 'ignore'
    
    /* delete files upon completion (I think) */
    /* This didn't work. */
    // cleanup = true

    afterScript 'rm *.bam*'


    input:
    tuple path(bam), val(total_cpus), val(mem_per_thread)

    script:

    avail_mem = total_cpus * mem_per_thread

    """
    echo ""
    echo "n CPUs: $total_cpus"
    echo "Total mem: $task.memory"
    echo "mem_per_thread: $mem_per_thread"
    echo ""

    sambamba sort \\
        -t "${task.cpus}" \\
        -m ${avail_mem}G \\
        -o "${bam.baseName}.csorted.bam" \\
        -l 2 \\
        --tmpdir \$PWD \\
        "${bam}"

    sambamba index \\
        -t "${task.cpus}" \\
        --show-progress \\
        "${bam.baseName}.csorted.bam"

    """
}




def generate_mem_and_cpu_tuples() {

    /*************************/
    /* Low memory per thread */
    /*************************/

    /*
     * Create list of CPUs and total memory to test. I want to test CPUs
     * from 1 to 7 because that's where we see the most gains in original
     * results. Testing MEM from 1 to 21 by 2.
     */
    low_mem_and_cpu_list = []
    (1..7).each{
        cpu ->
            1.step(22, 2){
                mem_per_thread -> 
                    low_mem_and_cpu_list.add( [cpu, mem_per_thread] )
            }
    }

    /*
     * Test CPUs from 9 to 17 by 2. Testing MEM from 1 to 21 by 2.
     */
    9.step(18, 2){
        cpu ->
            1.step(22, 2){
                mem_per_thread -> 
                    low_mem_and_cpu_list.add( [cpu, mem_per_thread] )
            }
    }

    /*
     * Create list of CPUs and total memory to test. I want to test CPUs from 1
     * to 17 with step size == 2. The specified value ('18') is not inclusive. I'll
     * test memory (per thread) from 1 to 25 with step size == 2. The specified
     * value ('26') is not inclusive.
     */
//    1.step(18, 2){
//        cpu ->
//            1.step(26, 2){
//                mem_per_thread -> 
//                    low_mem_and_cpu_list.add( [cpu, mem_per_thread] )
//            }
//    }

    low_mem_and_cpu_ch = Channel.from( low_mem_and_cpu_list )



    /****************************/
    /* Medium memory per thread */
    /****************************/

    /*
     * Create list of CPUs and total memory to test. I want to test CPUS from 1
     * to 10 with step size == 3. The specified value ('11') is not inclusive. I'll
     * test memory from 27 to 41 with step size == 3. The specified value ('42')
     * is not inclusive.
     */
//    med_mem_and_cpu_list = []
//    1.step(11, 3){
//        cpu ->
//            27.step(42, 3){
//                mem_per_thread ->
//                    med_mem_and_cpu_list.add( [cpu, mem_per_thread] )
//            }
//    }
//
//    med_mem_and_cpu_ch = Channel.from( med_mem_and_cpu_list )




    /**************************/
    /* High memory per thread */
    /**************************/

    /*
     * Create list of CPUs and total memory to test. I want to test CPUs from 1
     * to 4 with step size == 1. The specified value ('6') is not inclusive. I'll
     * test memory from 60 to 100 with step size == 10. The specified value ('101')
     * is not inclusive.
     */
    high_mem_and_cpu_list = []
    1.step(5, 1){
        cpu ->
            60.step(101, 10){
                mem_per_thread ->
                    high_mem_and_cpu_list.add( [cpu, mem_per_thread] )
            }
    }

    high_mem_and_cpu_ch = Channel.from( high_mem_and_cpu_list )



    /*
     * Concat all mem and CPU tuples
     */
    all_mem_and_cpu_ch = low_mem_and_cpu_ch
        // | concat( med_mem_and_cpu_ch )
        | concat( high_mem_and_cpu_ch )

    return all_mem_and_cpu_ch

}
