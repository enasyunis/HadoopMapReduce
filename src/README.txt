// add to .bashrc
$HADOOP_HOME = /opt/hadoop-0.20.1
// add to path
$HADOOP_HOME/bin

***********************************************
mkdir ~/assign0 ~/assign0/input
cp $HADOOP_HOME/conf/*.xml ~/assign0/input/.

cd ~/assign0
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-*-examples.jar wordcount input output2
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-*-examples.jar grep input output 'dfs[a-z.]+'
**********************************************
After compiling the source files

mkdir ~/assign1 ~/assign1/input
cp $HADOOP_HOME/conf/*.xml ~/assign1/input/.
jar -cvf ~/assign1/wordcount.jar -C ~/workspace/OS/bin/ .

cd ~/assign1
$HADOOP_HOME/bin/hadoop jar wordcount.jar sa.edu.kaust.advos.WordCount input output_A
$HADOOP_HOME/bin/hadoop jar wordcount.jar sa.edu.kaust.advos.WordCountWCombiner input output_B
$HADOOP_HOME/bin/hadoop jar wordcount.jar sa.edu.kaust.advos.WordCountWMapComb input output_C
***********************************************
mkdir ~/assign2 ~/assign2/input
cp $HADOOP_HOME/conf/*.xml ~/assign2/input/.
jar -cf ~/assign2/ordroccur.jar -C ~/workspace/OS/bin/ .

cd ~/assign2
$HADOOP_HOME/bin/hadoop jar ordroccur.jar sa.edu.kaust.advos.OrdrCoOccur 4 input output_A
