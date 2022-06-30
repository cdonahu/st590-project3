import pandas as pd
import math
import time

all_accs = pd.read_csv("data/all_accelerometer_data_pids_13.csv")


SA0297_df = all_accs.loc[all_accs.pid=='SA0297']
PC6771_df = all_accs.loc[all_accs.pid=='PC6771']

position = 0
step=500
for i in range (0, math.floor(min(SA0297_df.shape[0], PC6771_df.shape[0])/500)):
    
    print ("Position " + str(position))
    print ("Step " + str(step))
    
    output_SA0297 = SA0297_df.iloc[position:step+position]
    output_PC6771 = PC6771_df.iloc[position:step+position]
    
    output_SA0297.to_csv("sa0297files/sa0927_" + str(i) + ".csv", index = False, header = False)
    output_PC6771.to_csv("pc6771files/pc6771_" + str(i) + ".csv", index = False, header = False)
    time.sleep(25)
    position = position+step
    
    
from pyspark.sql.types import StructType

myschema = StructType() \
.add("time", "string").add("pid", "string") \
.add("x", "float").add("y", "float").add("z", "float")


df1 = spark \
.readStream \
.schema(myschema) \
.csv("sa0297files/")  

agg_df1 = df1.select('time', 'pid', (pow( (df1['x']*df1['x'] + df1['y']*df1['y'] + df1['z']*df1['z']), 0.5)))
q1 = agg_df1.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_SA0297/") \
.option("checkpointLocation","checkpoints") \
.start()


df2 = spark \
.readStream \
.schema(myschema)
.csv("pc6771files/") 

agg_df2 = df2.select('time', 'pid', (pow( (df1['x']*df1['x'] + df1['y']*df1['y'] + df1['z']*df1['z']), 0.5)))

q2 = agg_df2.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "data/output_PC6771/") \
.option("checkpointLocation","checkpoints") \
.start()


allfiles = spark.read.option("header","false").csv("data/output_SA0297/part-*.csv") # Output as CSV file
allfiles \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("final_output_SA0297/single_csv_file/")


allfiles = spark.read.option("header","false").csv("data/output_PC6771/part-*.csv") # Output as CSV file
allfiles \
.coalesce(1) \
.write.format("csv") \
.option("header", "false") \
.save("final_output_PC6771/single_csv_file/")

