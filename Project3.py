#######################################################
# ST - 590
# Project 3
# Performed by Claudia Donahue and Nataliya Peshekhodko
#######################################################

import pandas as pd # To work with data frames
import math         # For ceiling function in for-loop
import time         # Used to sleep for-loop between iterations
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#######################################################
# Set up for Creating Files
#######################################################

# Read in the .csv files from https://archive.ics.uci.edu/ml/datasets/Bar+Crawl%3A+Detecting+Heavy+Drinking
# Data we want is the cell phone accelerometer data
all_accs = pd.read_csv("data/all_accelerometer_data_pids_13.csv")

# Create two data frames, one for person SA0297’s data and one for person PC6771’s data
SA0297_df = all_accs.loc[all_accs.pid=='SA0297']
PC6771_df = all_accs.loc[all_accs.pid=='PC6771']

# Initiate variables used in for-loop
position = 0
step=500

# Calculate the number of iterations based on whichever person has more entries
# and 500 entries per iteration
print ('Max number of iterations ' + str (math.ceil(max(SA0297_df.shape[0], PC6771_df.shape[0])/500)))

# Set up for-loop to write 500 values at a time from the first line
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
    
 # output data to a .csv file in a folder for each person   
    if not output_SA0297.empty:
        output_SA0297.to_csv("sa0297_csv_files/sa0927_" + str(i) + ".csv", index = False, header = False)
        
    if not output_PC6771.empty:  
        output_PC6771.to_csv("pc6771_csv_files/pc6771_" + str(i) + ".csv", index = False, header = False)

# The loop should then delay for 20 seconds after writing to the files
    time.sleep(25)
    position = position+step
    
    output_PC6771 = pd.DataFrame ()
    output_SA0297 = pd.DataFrame ()
    
    
#######################################################
# Reading a Stream
#######################################################
from pyspark.sql.types import StructType

# Set up the schema
myschema = StructType() \
.add("time", "long") \
.add("pid", "string") \
.add("x", "float") \
.add("y", "float") \
.add("z", "float")

# Create an input stream from the csv folder for each person
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

# Do a basic transformation of the x, y, and z coordinates into a magnitude
# Keep the time and pid columns, this new column, and drop the original x, y, and z columns
agg_df1 = df1.select('time', 'pid', (pow( (df1['x']*df1['x'] + df1['y']*df1['y'] + df1['z']*df1['z']), 0.5)))

agg_df2 = df2.select('time', 'pid', (pow( (df2['x']*df2['x'] + df2['y']*df2['y'] + df2['z']*df2['z']), 0.5)))


#######################################################
# Writing the Streams
#######################################################

# Write each stream out to their own csv file
# Use the append outputMode, the csv output format
# Include option for checkpointlocation
q1 = agg_df1.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/output_SA0297/") \
    .option("checkpointLocation","checkpoints") \
    .start()                                     # Start the query

q2 = agg_df2.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_PC6771/") \
.option("checkpointLocation","checkpoints_2") \
.start()

# To stop the streams
q1.stop()
q2.stop()

#######################################################
# Read in all "part" files
#######################################################

# Read in all the pieces for each person using PySpark
allfiles_sa = spark \
.read.option("header","false") \
.csv("data/output_SA0297/part-*.csv") 

allfiles_pc = spark \
.read.option("header","false") \
.csv("data/output_PC6771/part-*.csv") 

#######################################################
# Output to single CSV file
#######################################################

# Output each to their own single .csv file
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