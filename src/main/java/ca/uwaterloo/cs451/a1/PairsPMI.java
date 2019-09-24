package ca.uwaterloo.cs451.a1;

import org.apache.hadoop.mapreduce.Partitioner;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloats;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import java.lang.Math;
import java.io.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.nio.file.*; 


/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
	

  // Mapper: emits (token, 1) for every word occurrence.
  public static final class MyMapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
	Set<String> hash_Set = new HashSet<String>();
	    int i = 0;
      for (String word : Tokenizer.tokenize(value.toString())) {
	      if(i >= 40)
		      break;
	      hash_Set.add(word);
	      i += 1;
      }
	    for(String word : hash_Set){
        WORD.set(word);
        context.write(WORD, ONE);
	    }
      }
    }

	
	public static final class MyReducerWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
}

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfFloats> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final PairOfFloats ONE = new PairOfFloats(1,1);
    private int window = 2;

    @Override
    public void setup(Context context) {
      window = context.getConfiguration().getInt("window", 2);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
	    Set<String> hash_Set = new HashSet<String>();
	    int i=0;
	    for (String word : Tokenizer.tokenize(value.toString())) {
		    if(i >= 40)
			    break;
	      hash_Set.add(word);
		    i += 1;
      }
	   
	    String[] tokens = new String[40];
	    int size = 0;
	for(String word : hash_Set)
	{
		tokens[size] = word;
		size += 1;
	}

      for (i = 0; i < size; i++) {
        for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, size); j++) {
          if (i == j) continue;
          PAIR.set(tokens[i], tokens[j]);
          context.write(PAIR, ONE);
        }
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStrings, PairOfFloats, PairOfStrings, PairOfFloats> {
//     private static final IntWritable SUM = new IntWritable();
	  private static final PairOfFloats PMI = new PairOfFloats(1,1);
	  private static Map<String, Integer> total = new HashMap<String, Integer>();
	  private static int totalSum = 0;
	  	  private static int threshold = 0;


	  
	  
	  @Override
    public void setup(Context context) throws IOException{
      //TODO Read from intermediate output of first job
      // and build in-memory map of terms to their individual totals
	    	    threshold = context.getConfiguration().getInt("threshold", 3);

      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      
      String yoPath = conf.get("intermediatePath");
      Path filePath = new Path(yoPath + "/part-r-00000");
	    

      if(!fs.exists(filePath)){
        throw new IOException("File Not Found: ");
      }
      
      BufferedReader reader = null;
      try{
        FSDataInputStream fin = fs.open(filePath);
        InputStreamReader inStream = new InputStreamReader(fin);
        reader = new BufferedReader(inStream);
        
      } catch(FileNotFoundException e){
        throw new IOException("Can not open file");
      }
      
      
      String line = reader.readLine();
      while(line != null){
	      totalSum += 1;
        
        String[] parts = line.split("\\s+");
        if(parts.length != 2){
          LOG.info("incorrect format");
        } else {
          total.put(parts[0], Integer.parseInt(parts[1]));
        }
        line = reader.readLine();
      }
      
      reader.close();
      
    }
	  
	public double getLog(double num){
		double log = Math.log(num)/Math.log(10);
		return(log);
	}
	  
   // @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloats> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfFloats> iter = values.iterator();
      float sum = 0;
	    double pmi = 1;
      while (iter.hasNext()) {
        sum += iter.next().getLeftElement();
      }
	    if(sum > threshold){
		    String x = key.getLeftElement();
		    String y = key.getRightElement();
		    if(!x.equals(y))
		    {
		    double Both = sum;
        	    int X = total.get(x);
        	    int Y = total.get(y);

        	    pmi = ((Both * totalSum) / (X * Y));
// 		    pmi = getLog(pmi);
		    pmi = (double)Math.log10(pmi);
// 		    System.out.println(pmi);
		    float fpmi = (float)pmi;
		    PMI.set(sum, fpmi);
		    key.set(x,y);

      		   context.write(key, PMI);
		    }
    }
  }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, PairOfFloats> {
    @Override
    public int getPartition(PairOfStrings key, PairOfFloats value, int numReduceTasks) {
	    
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
  
  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;
	  
    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold limit")
	    int threshold = 0;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);
	  LOG.info(" - threshold: " + args.threshold);
	  
	  
	  
	  
	  Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(PairsPMI.class.getSimpleName() + "WordCount");
    job1.setJarByClass(PairsPMI.class);

    job1.setNumReduceTasks(1);
	  String tempPath = "temp";
    Path tempDir = new Path(tempPath);
    conf.set("intermediatePath", tempPath);


    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(tempPath));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyMapperWordCount.class);
    job1.setCombinerClass(MyReducerWordCount.class);
    job1.setReducerClass(MyReducerWordCount.class);

    // Delete the output directory if it exists already.
    
    Path outputDir = tempDir;
    FileSystem.get(conf).delete(tempDir, true);

    long startTime = System.currentTimeMillis();
    long totalTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	  
	  
	  

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI.class.getSimpleName() + "final output");
    job.setJarByClass(PairsPMI.class);

    // Delete the output directory if it exists already.
    Path outputDir1 = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir1, true);

    job.getConfiguration().setInt("window", args.window);
	  job.getConfiguration().setInt("threshold", args.threshold);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(PairOfFloats.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(PairOfFloats.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
 
    System.out.println("Total Time " + (System.currentTimeMillis() - totalTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
