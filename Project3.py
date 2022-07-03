#######################################################
# ST - 590
# Project 3
# Performed by Claudia Donahue and Nataliya Peshekhodko
#######################################################

import pandas as pd
import math
import time

#######################################################
# Set up for Creating Files
#######################################################
all_accs = pd.read_csv("data/all_accelerometer_data_pids_13.csv")
SA0297_df = all_accs.loc[all_accs.pid=='SA0297']
PC6771_df = all_accs.loc[all_accs.pid=='PC6771']

position = 0
step=500

print ('Max number of iterations ' + str (math.ceil(max(SA0297_df.shape[0], PC6771_df.shape[0])/500)))

for i in range (0, math.ceil(max(SA0297_df.shape[0], PC6771_df.shape[0])/500)):
    
    print ("Position " + str(position))
    print ("Step " + str(step))
    
    if step+position > SA0297_df.shape[0]:
        if position < SA0297_df.shape[0]:
            output_SA0297 = SA0297_df.iloc[position:SA0297_df.shape[0]]
    else:
        output_SA0297 = SA0297_df.iloc[position:step+position]
        
        
    if step+position > PC6771_df.shape[0]:
        if position < PC6771_df.shape[0]:
            output_PC6771 = PC6771_df.iloc[position:PC6771_df.shape[0]]
    else:
        output_PC6771 = PC6771_df.iloc[position:step+position]
    
    
    if not output_SA0297.empty:
        output_SA0297.to_csv("sa0297_csv_files/sa0927_" + str(i) + ".csv", index = False, header = False)
        
    if not output_PC6771.empty:  
        output_PC6771.to_csv("pc6771_csv_files/pc6771_" + str(i) + ".csv", index = False, header = False)
        
    time.sleep(25)
    position = position+step
    
    output_PC6771 = pd.DataFrame ()
    output_SA0297 = pd.DataFrame ()
    
    
#######################################################
# Reading a Stream
#######################################################
from pyspark.sql.types import StructType

myschema = StructType() \
.add("time", "long") \
.add("pid", "string") \
.add("x", "float") \
.add("y", "float") \
.add("z", "float")

df1 = spark \
.readStream \
.schema(myschema) \
.csv("sa0297_csv_files/")

df2 = spark \
.readStream \
.schema(myschema) \
.csv("pc6771_csv_files/") 

#######################################################
# Transform/Aggregation Step
#######################################################

agg_df1 = df1.select('time', 'pid', (pow( (df1['x']*df1['x'] + df1['y']*df1['y'] + df1['z']*df1['z']), 0.5)))

agg_df2 = df2.select('time', 'pid', (pow( (df2['x']*df2['x'] + df2['y']*df2['y'] + df2['z']*df2['z']), 0.5)))


#######################################################
# Writing the Streams
#######################################################

q1 = agg_df1.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_SA0297/") \
.option("checkpointLocation","checkpoints") \
.start()

q2 = agg_df2.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_PC6771/") \
.option("checkpointLocation","checkpoints_2") \
.start()

#######################################################
# Read in all "part" files
#######################################################
allfiles_sa = spark \
.read.option("header","false") \
.csv("data/output_SA0297/part-*.csv") 

allfiles_pc = spark \
.read.option("header","false") \
.csv("data/output_PC6771/part-*.csv") 

#######################################################
# Output to single CSV file
#######################################################
allfiles_sa \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("final_output_SA0297/single_csv_file/")

allfiles_pc \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("final_output_PC6771/single_csv_file/")