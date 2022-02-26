
import plotly.express as px
import pandas as pd
import re
import sys

def generate_dataframe(trace_file):
    software_list = []
    sample_name_list = []
    sample_type_list = []
    cpus_list = []
    perc_cpu_list = []
    rss_list = []
    perc_mem_list = []
    mem_per_thread_list = []
    status_list = []
    realtime_list = []
    peak_memory_list = []
    peak_vmem_list = []
    with open(trace_file, 'rt') as trace:
        with open(trace_file + '.tokenized.txt', 'wt') as out:
            for line in trace:
                if line.startswith('task_id'):
                    toks = line.strip().split('\t')
                    toks.insert(5, 'Software')
                    toks.insert(6, 'Sample')
                    toks.insert(7, 'CPUs')
                    toks.insert(8, 'Mem per thread')
                    out.write('\t'.join(toks) + '\n')
                    continue

                toks = line.strip().split('\t')

                status = toks[4]

                # Ignore FAILED
                if status == "COMPLETED":
                    name = toks[3]

                    name_toks = name.split(';')
                    software = name_toks[0].split(' ')[0].replace('_csort_proc', '')
                    sample = name_toks[0].split(' ')[1].replace('(', '')
                    cpu = int(name_toks[1].split(':')[1])
                    mem_per_thread = int(name_toks[2].split(':')[1].replace('GB)', ''))

                    realtime_toks = toks[8].split(' ')

                    # If there are three tokens, we have hours, minutes, and seconds
                    if len(realtime_toks) == 3:

                        # Convert hours to seconds
                        hours = float(realtime_toks[0].replace('h', '')) * 60
                        minutes = float(realtime_toks[1].replace('m', ''))
                        seconds = float(realtime_toks[2].replace('s', ''))

                        realtime = round(hours + minutes + seconds / float(60), 2)

                    # If there are two tokens, we have one of the following:
                    #  1. hours and minutes
                    #  2. hours and seconds
                    #  3. minutes and seconds
                    elif len(realtime_toks) == 2:

                        hours_or_minutes = realtime_toks[0]
                        minutes_or_seconds = realtime_toks[1]

                        # If hours, convert to minutes
                        if 'h' in hours_or_minutes:
                            hours_or_minutes = int(hours_or_minutes.replace('h', '')) * 60
                        elif 'm' in hours_or_minutes:
                            hours_or_minutes = int(hours_or_minutes.replace('m', ''))

                        # If 'm', we have minutes
                        if 'm' in minutes_or_seconds:
                            minutes_or_seconds = int(minutes_or_seconds.replace('m', ''))
                            realtime = hours_or_minutes + minutes_or_seconds

                        # we have seconds
                        elif 's' in minutes_or_seconds:
                            minutes_or_seconds = int(minutes_or_seconds.replace('s', ''))
                            realtime = round(hours_or_minutes + minutes_or_seconds / float(60), 2)

                    # Must have one token, which could technically be hours, minutes or seconds,
                    # but shouldn't ever be seconds in this test.
                    else:
                        time = realtime_toks[0]

                        if 'h' in time:
                            realtime = int(time.replace('h', '')) * 60
                        elif 'm' in time:
                            realtime = int(time.replace('m', ''))
                        else:
                            realtime = round(time / float(60), 2)


                    perc_cpu = float(toks[9].replace('%',''))
                    rss = float(toks[10].replace(' GB', ''))
                    perc_mem = float(toks[11].replace('%',''))
                    peak_mem = float(toks[12].replace(' GB', ''))
                    peak_vmem = float(toks[13].replace(' GB', ''))

                    # Add extracted data to the line toks for printing to file
                    toks.insert(5, software)
                    toks.insert(6, sample)
                    toks.insert(7, str(cpu))
                    toks.insert(8, str(mem_per_thread))
                    out.write('\t'.join(toks) + '\n')

                    # CU003128 was re-aligned
                    if 'CU003128' in sample:
                        sample_type_list.append('REALIGN')
                    else:
                        sample_type_list.append('COLLATED')
                            
                    sample_name_list.append(sample)
                    software_list.append(software)
                    cpus_list.append(cpu)
                    perc_cpu_list.append(perc_cpu)
                    rss_list.append(rss)
                    perc_mem_list.append(perc_mem)
                    mem_per_thread_list.append(mem_per_thread)
                    realtime_list.append(realtime)
                    peak_memory_list.append(peak_mem)
                    peak_vmem_list.append(peak_vmem)


    data = {
            'Software': software_list,
            'Sample Name': sample_name_list,
            'Sample Prep (collated or aligned)': sample_type_list,
            'CPUs': cpus_list,
            'Percent CPU': perc_cpu_list,
            'RSS': rss_list,
            'Percent Mem': perc_mem_list,
            'Mem Per Thread (GB)': mem_per_thread_list,
            'Real time (minutes)': realtime_list,
            'Peak Total Mem (GB)': peak_memory_list,
            'Peak Total vMem (GB)': peak_vmem_list
           }
    return pd.DataFrame(data)



def main(trace_file):
    df = generate_dataframe(trace_file)
    print(df)

    print(df.groupby(['Software', 'CPUs'])['Real time (minutes)'].describe())


    # Real time by CPUs and Mem Per Thread, colored by Sample Prep
    rt_prep = px.scatter_3d(df, x='CPUs', y='Mem Per Thread (GB)', z='Real time (minutes)',
                  color='Sample Prep (collated or aligned)', opacity=0.7)
    rt_prep.show()
    rt_prep.update_layout(margin=dict(l=0, r=0, b=0, t=0))


    # Real time by CPUs and Mem Per Thread, colored by software
    rt_soft = px.scatter_3d(df, x='CPUs', y='Mem Per Thread (GB)', z='Real time (minutes)',
                  color='Software', opacity=0.7)
    rt_soft.show()
    rt_soft.update_layout(margin=dict(l=0, r=0, b=0, t=0))


    # CPU by Perc CPU to measure ability to utilize additional CPUs
    perc_cpu = px.box(df, x='CPUs', y='Percent CPU', color='Software', points='all')
    perc_cpu.show()

    # Mem per thread by Perc mem to measure ability to utilize additional memory
    perc_mem = px.scatter_3d(df, x='Mem Per Thread (GB)', y='CPUs', z='Percent Mem', color='Software',
            opacity=0.7)
    perc_mem.show()


if __name__ == "__main__":
    main(sys.argv[1])
