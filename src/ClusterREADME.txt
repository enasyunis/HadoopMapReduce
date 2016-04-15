hadoop jar /opt/hadoop/hadoop-0.20.1/hadoop*examples.jar wordcount /shared/sample-input output


hadoop dfs ... for the different hdfs commands.
	-ls <name of folder>
	-cat  <name of file>

hadoop jar wordcount.jar sa.edu.kaust.advos.OrgWordCount /shared/sample‐input output

hadoop jar wordcount.jar sa.edu.kaust.advos.WordCount hdfs://10.68.202.54/shared/sample‐input output_At
hadoop jar wordcount.jar sa.edu.kaust.advos.WordCountWCombiner /shared/sample-input output_B
hadoop jar wordcount.jar sa.edu.kaust.advos.WordCountWMapComb /shared/sample‐input output_C

% Webapp
http://10.68.202.54:50030