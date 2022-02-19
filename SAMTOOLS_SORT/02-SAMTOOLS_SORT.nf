
/*
 * Make this pipeline a nextflow 2 implementation
 */
nextflow.enable.dsl=2

input_files = [
               file('../REALIGN_SAMPLE/A-CUHS-CU003128-BL-COL-49696BL1.unsorted.bam'),
               file('../COLLATE_SAMPLES/A-CUHS-CU000208-BL-COL-56227BL1_vcpa1.0_GRU-IRB-PUB.collated.bam'),
               file('../COLLATE_SAMPLES/A-CUHS-CU002707-BL-COL-40848BL1_vcpa1.0_GRU-IRB-PUB.collated.bam')
              ]


/*************************/
/* Low memory per thread */
/*************************/

/*
 * Create list of CPUs and total memory to test. I want to test CPUs from 1
 * to 17 with step size == 2. The specified value ('18') is not inclusive. I'll
 * test memory (per thread) from 1 to 25 with step size == 2. The specified
 * value ('26') is not inclusive.
 */
low_mem_and_cpu_list = []
1.step(18, 2){
    cpu ->
        1.step(26, 2){
            mem_per_thread -> 
                low_mem_and_cpu_list.add( [cpu, mem_per_thread] )
        }
}

Channel.from( low_mem_and_cpu_list )
    | set { low_mem_and_cpu_ch } 



/****************************/
/* Medium memory per thread */
/****************************/

/*
 * Create list of CPUs and total memory to test. I want to test CPUS from 1
 * to 10 with step size == 3. The specified value ('11') is not inclusive. I'll
 * test memory from 27 to 41 with step size == 3. The specified value ('42')
 * is not inclusive.
 */
med_mem_and_cpu_list = []
1.step(11, 3){
    cpu ->
        27.step(42, 3){
            mem_per_thread ->
                med_mem_and_cpu_list.add( [cpu, mem_per_thread] )
        }
}



Channel.from( med_mem_and_cpu_list )
    | set { med_mem_and_cpu_ch }




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

Channel.from( high_mem_and_cpu_list )
    | set { high_mem_and_cpu_ch }



/*
 * Concat all mem and CPU tuples
 */
low_mem_and_cpu_ch
    | concat( med_mem_and_cpu_ch )
    | concat( high_mem_and_cpu_ch )
    | set { all_mem_and_cpu_ch }


/*
 * Combine input files with mem & CPU tuples
 */
Channel.from(input_files)
    | combine( all_mem_and_cpu_ch )
    | set { input_file_ch }


workflow {

//    test_input = Channel.of( [file('../REALIGN_SAMPLE/A-CUHS-CU003128-BL-COL-49696BL1.unsorted.bam'), 1, 1] )
//                    .view()
//    samtools_coordinate_sort_proc( test_input )
    samtools_coordinate_sort_proc( input_file_ch )
}


process samtools_coordinate_sort_proc {

    tag { "${bam.baseName};CPUS:${total_cpus};MEM_PER_THREAD:${mem_per_thread}GB" }

    executor='slurm'
    queue = 'normal'
    cpus { total_cpus }


    /*
     * In my experience, samtools always exceeds its allotted memory. Creating
     * a 10% buffer
     */
    memory { total_cpus * mem_per_thread.GB * 1.2}
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


    println ""
    println "n CPUs: $total_cpus"
    println "Total mem: $task.memory"
    println "mem_per_thread: $mem_per_thread"
    println ""

    """
    samtools sort \\
        -@ "${additional_threads}" \\
        -m ${mem_per_thread}G \\
        -o "${bam.baseName}.csorted.bam" \\
        -T "${bam.baseName}.csorted" \\
        --write-index \\
        "${bam}"
    """
}
