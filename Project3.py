#######################################################
# ST - 590
# Project 3
# Performed by Claudia Donahue and Nataliya Peshekhodko
#
# In this project, we will use pyspark to monitor a
# folder, read in data that arrives, transform the data,
# and write the results out to a file. 
#######################################################

import pandas as pd
import math
import time

#######################################################
# Set up for creating Files: 
# We are using data from a study that tried to detect
# heavy drinking during a "bar crawl" based partly on 
# readings from cell phone accelerometer data.
#######################################################
# The data was dowloaded from https://archive.ics.uci.edu/ml/datasets/Bar+Crawl%3A+Detecting+Heavy+Drinking

# Read all_accelerometer data from .csv file and store it to data frame
all_accs = pd.read_csv("data/all_accelerometer_data_pids_13.csv")

# We are just looking at two individuals, SA0297 and PC6771. 
# Create two separate data frames for pid=SA0297 and pid=PC677
SA0297_df = all_accs.loc[all_accs.pid=='SA0297']
PC6771_df = all_accs.loc[all_accs.pid=='PC6771']

# Set up loop to write 500 values at a time for both PIDs
# Start from the first raw with step size = 500 raws
# Define starting position=0 and step size step=500 rows 
position = 0
step=500

# Loop will read all available data for both PIDs
# Number of iterations defined as smallest integer greater than or equal to max number of 
# rows for both PIDs divided by the step size
for i in range (0, math.ceil(max(SA0297_df.shape[0], PC6771_df.shape[0])/step)):
 
    # Check if there are rows available to read for SA0297
    if step+position > SA0297_df.shape[0]:
        if position < SA0297_df.shape[0]:
            output_SA0297 = SA0297_df.iloc[position:SA0297_df.shape[0]]
    else:
        output_SA0297 = SA0297_df.iloc[position:step+position]
    
    # Check if there are rows available to read for PC6771
    if step+position > PC6771_df.shape[0]:
        if position < PC6771_df.shape[0]:
            output_PC6771 = PC6771_df.iloc[position:PC6771_df.shape[0]]
    else:
        output_PC6771 = PC6771_df.iloc[position:step+position]
    
    # Output to csv only if there is data in data frame for each PID
    if not output_SA0297.empty:
        output_SA0297.to_csv("sa0297_csv_files/sa0927_" + str(i) + ".csv", index = False, header = False)
        
    if not output_PC6771.empty:  
        output_PC6771.to_csv("pc6771_csv_files/pc6771_" + str(i) + ".csv", index = False, header = False)
        
    # Set up 20-second time delay between outputs    
    time.sleep(20)
    
    # Update starting point for next reading
    position = position+step
    
    # Clean up data data frames before next iteration
    output_PC6771 = pd.DataFrame ()
    output_SA0297 = pd.DataFrame ()
    
    
#######################################################
# Reading a Stream
#######################################################
from pyspark.sql.types import StructType

# Set up schema for input stream
# This schema will be used by both input streams for pid=SA0297 and for pid=PC6771
myschema = StructType() \
.add("time", "long") \
.add("pid", "string") \
.add("x", "float") \
.add("y", "float") \
.add("z", "float")

# Create input stream for folder with .csv files for pid=SA0297
df1 = spark \
.readStream \
.schema(myschema) \
.csv("sa0297_csv_files/")

# Create input stream for folder with .csv files for pid=PC6771
df2 = spark \
.readStream \
.schema(myschema) \
.csv("pc6771_csv_files/") 

#######################################################
# Transform/Aggregation Step
#######################################################

# For each stream apply transformation of the x,y and z coordinates into a magnitude
agg_df1 = df1.select('time', 'pid', (pow( (df1['x']*df1['x'] + df1['y']*df1['y'] + df1['z']*df1['z']), 0.5)))

agg_df2 = df2.select('time', 'pid', (pow( (df2['x']*df2['x'] + df2['y']*df2['y'] + df2['z']*df2['z']), 0.5)))

# We just want to keep the columns for Time, PID, and magnitude

#######################################################
# Writing the Streams
#######################################################

# Write each stream out to their own csv files
# Write stream for PID=SA0297
q1 = agg_df1.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_SA0297/") \
.option("checkpointLocation","checkpoints") \
.start()

# Write stream for PID=PC6771
q2 = agg_df2.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_PC6771/") \
.option("checkpointLocation","checkpoints_2") \
.start()

#######################################################
# Read in all "part" files
#######################################################

# Read in all .csv files for SA0297
allfiles_sa = spark \
.read.option("header","false") \
.csv("data/output_SA0297/part-*.csv") 

# Read in all .csv files for PC6771
allfiles_pc = spark \
.read.option("header","false") \
.csv("data/output_PC6771/part-*.csv") 

#######################################################
# Output to single CSV file
#######################################################

# Single output for PID=SA0297 into dedicated directory final_output_SA0297/single_csv_file/
allfiles_sa \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("final_output_SA0297/single_csv_file/")

# Single output for PID=PC6771 into dedicated directory final_output_PC6771/single_csv_file
allfiles_pc \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("final_output_PC6771/single_csv_file/")