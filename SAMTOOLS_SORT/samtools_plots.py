
import plotly.express as px
import pandas as pd
import re

sample_type_list = []
cpus_list = []
mem_per_thread_list = []
status_list = []
duration_list = []
peak_memory_list = []
peak_vmem_list = []
with open('trace.txt', 'rt') as traces:
    for line in traces:
        if line.startswith('task_id'):
            continue

        # print(line)

        toks = line.strip().split('\t')

        status = toks[4]

        # Ignore FAILED
        if status == "COMPLETED":
            name = toks[3]

            name_toks = name.split(';')
            sample = name_toks[0]
            cpu = int(name_toks[1].split(':')[1])
            mem_per_thread = int(name_toks[2].split(':')[1].replace('GB)', ''))

            duration_toks = toks[7].split(' ')

            # If there are three tokens, we have hours, minutes, and seconds
            if len(duration_toks) == 3:

                # Convert hours to seconds
                hours = float(duration_toks[0].replace('h', '')) * 60
                minutes = float(duration_toks[1].replace('m', ''))
                seconds = float(duration_toks[2].replace('s', ''))

                duration = round(hours + minutes + seconds / float(60), 2)

            # If there are two tokens, we have one of the following:
            #  1. hours and minutes
            #  2. hours and seconds
            #  3. minutes and seconds
            elif len(duration_toks) == 2:

                hours_or_minutes = duration_toks[0]
                minutes_or_seconds = duration_toks[1]

                # If hours, convert to minutes
                if 'h' in hours_or_minutes:
                    hours_or_minutes = int(hours_or_minutes.replace('h', '')) * 60
                elif 'm' in hours_or_minutes:
                    hours_or_minutes = int(hours_or_minutes.replace('m', ''))

                # If 'm', we have minutes
                if 'm' in minutes_or_seconds:
                    minutes_or_seconds = int(minutes_or_seconds.replace('m', ''))
                    duration = hours_or_minutes + minutes_or_seconds

                # we have seconds
                elif 's' in minutes_or_seconds:
                    minutes_or_seconds = int(minutes_or_seconds.replace('s', ''))
                    duration = round(hours_or_minutes + minutes_or_seconds / float(60), 2)

            # Must have one token, which could technically be hours, minutes or seconds,
            # but shouldn't ever be seconds in this test.
            else:
                time = duration_toks[0]

                if 'h' in time:
                    duration = int(time.replace('h', '')) * 60
                elif 'm' in time:
                    duration = int(time.replace('m', ''))
                else:
                    duration = round(time / float(60), 2)


            peak_mem = float(toks[10].replace(' GB', ''))
            peak_vmem = float(toks[11].replace(' GB', ''))


            # CU003128 was re-aligned
            if 'CU003128' in sample:
                sample_type_list.append('REALIGN')
            else:
                sample_type_list.append('COLLATED')
                    
            cpus_list.append(cpu)
            mem_per_thread_list.append(mem_per_thread)
            duration_list.append(duration)
            peak_memory_list.append(peak_mem)
            peak_vmem_list.append(peak_vmem)


data = {
        'Sample Prep': sample_type_list,
        'CPUs': cpus_list,
        'Mem Per Thread': mem_per_thread_list,
        'Duration': duration_list,
        'Peak Total Mem': peak_memory_list,
        'Peak Total vMem': peak_vmem_list
       }
df = pd.DataFrame(data)

print(df)

fig = px.scatter_3d(df, x='CPUs', y='Mem Per Thread', z='Duration',
              color='Sample Prep', opacity=0.7)

fig.show()

# tight layout
fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
