package sa.edu.kaust.advos;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map.Entry;

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

public class OrdrCoOccur {
	
  public static final String w_env_name = "Order.Co.Occurence.w";

  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntTuple, IntWritable>{

	  /*
	   * System assumes ASCII characters (base 8)
	   * System assumes w will never be larger than 2^16-1
	   * 
	   * My IntTuple will contain <char(a),char(b),short(w)>
	   */
	  
	    @Override 
	    protected void setup(Context context) throws IOException, InterruptedException {
	        System.err.println("M Setup* Conf.get : " + context.getConfiguration().get(w_env_name));
	    	w = Integer.parseInt(context.getConfiguration().get(w_env_name, "4"));
	    	map = new HashMap<IntTuple, IntWritable>();
	    }
	    
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	System.err.println("M* Cleanup Is called :)");
	    	for (Entry<IntTuple, IntWritable> me : map.entrySet()) {
	    		context.write(me.getKey(), me.getValue());
	    	}
	    	map = null;
			super.cleanup(context);
	    }
	    
    private IntWritable intWr;
    private String sentence;
    private int w;
    private int len;
    private char key1;
    private char key2;
    
    private HashMap<IntTuple, IntWritable> map;
    
    private void updateHashMap(IntTuple iT) {
    	intWr = map.get(iT);
    	if (intWr == null) { // does not exist
    		map.put(iT, new IntWritable(1));
    	} else {
    		intWr.set(intWr.get()+1);
    		map.put(iT, intWr);
    	}    	
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n\r\f"); // break on lines
      while (itr.hasMoreTokens()) {
    	  sentence = itr.nextToken().replaceAll("\\s", ""); // remove the spaces to ease calculations
    	  len = sentence.length();
    	  for (int i=0; i < len; i++) {
    		  key1 = sentence.charAt(i);
    		  for (int j=i+1; j < Math.min(w+i, len); j++){
    			  key2 = sentence.charAt(j);
    			  if (key1 != key2) { 
    				  updateHashMap(new IntTuple(key1, key2, j-i)); // a,b,j-i-1 case // in printing remember to add -1
    				  // updateHashMap(new IntTuple(key1, key2, 0));   // a,b,* case
    			  }
    		  }
       	  }  	  
      }
    }
   
  }
  
  public static class IntSumReducer 
       extends Reducer<IntTuple,IntWritable,Text,Text> {

    private LinkedHashMap<IntTuple, IntWritable> map;
    private Text t1, t2;
 
    @Override 
    protected void setup(Context context) throws IOException, InterruptedException {
    	map = new LinkedHashMap<IntTuple, IntWritable>();
    	t1 = new Text();
    	t2 = new Text();
    }
    
    private void emitHashMap(Context context) throws IOException, InterruptedException  {
    	// first check that map is not empty
    	if (map.size() == 0) {
    		return;
    	}
    	
    	String s = null;
    	StringBuilder sb = new StringBuilder();
    	for (IntTuple intT : map.keySet()) {
    		if (s == null) {
    			s = "<" + intT.getKey1() + "," + intT.getKey2() + ">";
    			sb.append("<");
    			sb.append(map.get(intT).get());
    			sb.append("[");
    		} else {
    			sb.append("<");
    			sb.append(intT.getKey3()-1); // correcting for number inbetween and not index difference
    			sb.append(",");
    			sb.append(map.get(intT).get());
    			sb.append(">");
    		}
    	}
    	sb.append("]>");
    	t1.set(s);
    	t2.set(sb.toString());
    	context.write(t1,t2);
    	map.clear();
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	System.err.println("R* Cleanup Is called :)");
    	
    	// emit the last set here... for sure we can :)
    	emitHashMap(context); 
    	
    	map = null;
		super.cleanup(context);
    }
    
    
    public void reduce(IntTuple key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
    	// add up all the values for abX
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      // start building for ab*
      IntTuple masterKey = new IntTuple(key.getKey1(), key.getKey2(), 0);

      // if ab* does not exist
      //    emit everything in map
      //    add ab* first
      // add abX to Map and increment ab*       
      
      if (! map.containsKey(masterKey)) { // if ab* does not exist
    	  emitHashMap(context); // emit everything in map
    	  map.put(masterKey, new IntWritable(sum)); // add ab* first
    	  map.put(key.clone(), new IntWritable(sum)); // add abX to map
      } else {
    	  IntWritable intWr = map.get(masterKey);
    	  intWr.set(intWr.get()+sum); // increment the value in the master
    	  map.put(masterKey, intWr); // increment ab*
    	  map.put(key.clone(), new IntWritable(sum)); // add abX to map
      }   
    }
  }

  // configure with -D mapred.reduce.tasks=26 -- 1 for each letter!!!
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 3) {
      System.err.println("Usage: OrdrCoOccur <w> <in> <out>");
      System.exit(2);
    }
    conf.set(w_env_name, otherArgs[0]);
    System.err.println("Main* Conf.get : " + conf.get(w_env_name));

    Job job = new Job(conf, "Ordered Co-Occurence");
    job.setJarByClass(OrdrCoOccur.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(5);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(IntTuple.class);
    job.setMapOutputValueClass(IntWritable.class);    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}