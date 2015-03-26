username: ywu014
node: z5

description of the jobs:
I divide the task into two mapreduce jobs.
1. Produce the temperature difference of each state: Using map-side join, for each valid record, mapper outputs {key = state, value = month&temperature}. The records of the same state go to the same reducer. And each reducer produces the average temperature of each of the 12 monthes of that state. Find the max and min value among the 12 temperatures and output the difference.
2. Produce a list of state, sorted by temperature difference: Use the temperature difference from job1 as the key to sort the states. Sorting is done by the embedded shuffle phase of mapreduce framework.

file structure
on cluster:
--project
----WeatherStationLocations.csv
----merge
------Driver.java
------Map.java
------Reduce.java
----sort
------Driver2.java
------Map2.java
------Reduce2.java

on hdfs:
--project
-----WeatherStationLocations.csv
-----input
-------2006.txt
-------2007.txt
-------2008.txt
-------2009.txt



------------- run the first job (runtime ~ 30s)-------------
1. cd /user/sjaco002/project/merge
2. javac -cp `hadoop classpath` Driver.java Map.java Reduce.java
3. jar -cvf merge.jar *
4. hadoop jar merge.jar Driver /user/sjaco002/project/input /user/sjaco002/project/recordingdata
------------- run the second job (runtime ~ 30s)--------------
5. cd ../sort
6. javac -cp `hadoop classpath` Driver2.java Map2.java Reduce2.java
7. jar -cvf sort.jar *
8. hadoop jar sort.jar Driver2 /user/sjaco002/project/recordingdata /user/sjaco002/project/output

