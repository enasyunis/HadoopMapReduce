package sa.edu.kaust.advos;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.HashMap;
import java.util.Map.Entry;

public class WordCountWMapComb {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, AlphaText, IntWritable>{

	    @Override 
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	map = new HashMap<AlphaText, IntWritable>();
	    }
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	System.err.println("Cleanup Is called :)");
	    	for (Entry<AlphaText, IntWritable> me : map.entrySet()) {
	    		context.write(me.getKey(), me.getValue());
	    	}
	    	map = null;
			super.cleanup(context);
	    }
	    
    private AlphaText word;
    private IntWritable intWr;

    private HashMap<AlphaText, IntWritable> map;
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word = AlphaText.createInLowerCase(itr.nextToken()); // opposite of avoid object creation... but that is the only way to keep a correct map!!!
        if (word.startsWithAlpha()) {
        	intWr = map.get(word);
        	if (intWr == null) {
        		map.put(word, new IntWritable(1));
        	} else {
        		intWr.set(intWr.get()+1);
        		map.put(word, intWr);
        	}
        }
      }
    }
   
  }
  
  public static class IntSumReducer 
       extends Reducer<AlphaText,IntWritable,AlphaText,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(AlphaText key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  // configure with -D mapred.reduce.tasks=26 -- 1 for each letter!!!
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCountWMapComb.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class); // without any combiners
    job.setNumReduceTasks(26);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(AlphaText.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
