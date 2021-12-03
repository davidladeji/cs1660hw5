import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private HashMap<Text, IntWritable> topWords = new HashMap<>();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        
        topWords.put(word, one);
      }
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Text, IntWritable> entry : topWords.entrySet()) 
        {
  
            Text word = entry.getKey();
            IntWritable count = entry.getValue();
  
            context.write(word, count);
        }
    }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	private int tmpFrequency = 0;
    private Text tmpWord = new Text("");
    
    private TreeMap<Integer, Text> topWordsReducer = new TreeMap<>();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      
      topWordsReducer.put(sum, key);
      
      
      if (topWordsReducer.size() > 5){
    	  topWordsReducer.remove(topWordsReducer.firstKey());
      }
      
      
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
    // write the word with the highest frequency
    	for (Integer count : topWordsReducer.keySet()) {
    		context.write(topWordsReducer.get(count), new IntWritable(count));
    }
  }
    
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}