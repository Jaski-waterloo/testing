/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a0;

import org.apache.hadoop.mapreduce.Partitioner;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloats;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
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
 
 
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  
  
  // Mapper: emits (token, 1) for every word occurrence.
  public static final class MyMapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
	Set<String> hash_Set = new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
	      hash_Set.add(word);
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
  

  private static final class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();

    private int window = 2;

    @Override
    public void setup(Context context) {
      window = context.getConfiguration().getInt("window", 2);
    }

//     @Override
    public void map(LongWritable key, String value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      for (int i = 0; i < tokens.size(); i++) {
        MAP.clear();
        for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.size()); j++) {
          if (i == j) continue;
          
          MAP.increment(tokens.get(j));
        }

        KEY.set(tokens.get(i));
        context.write(KEY, MAP);
      }
    }
  }
  

  private static final class MyReducer extends Reducer<String, HMapStIW, String, HashMap<Text, PairOfFloats>> {
  
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
        
        String[] parts = line.split("\\s+");
        if(parts.length != 2){
          LOG.info("incorrect format");
        } else {
          total.put(parts[0], Integer.parseInt(parts[1]));
		totalSum += Integer.parseInt(parts[1]);
        }
        line = reader.readLine();
      }
      
      reader.close();
      
    }
  
//     @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
	    HashMap<String, PairOfFloats> finalMap = new HashMap<String, PairOfFloats>();
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      float sum = 0;
      double pmi = 1;
      while (iter.hasNext()) {
      map.plus(iter.next());
      }
    for(String curKey : map.keySet()){
        if (map.get(curKey) < 10) continue;
	     String X = key.toString();

        double probPair = (double)map.get(curKey) / (double)totalSum;
        double probLeft = (double)total.get(X) / (double)totalSum;
        double probRight = (double)total.get(curKey) / (double)totalSum;

        pmi = Math.log10((double)probPair / ((double)probLeft * (double)probRight));
        float fpmi = (float)pmi;
        
        PMI.set(map.get(curKey), fpmi);
        finalMap.put(curKey,PMI);

      context.write(key.toString(), finalMap);
    }
  }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);
    
    
    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(StripesPMI.class.getSimpleName() + "WordCount");
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(args.numReducers);
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
    job.setJobName(StripesPMI.class.getSimpleName() + "Final Output");
    job.setJarByClass(StripesPMI.class);

    // Delete the output directory if it exists already.
    outputDir = Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job.getConfiguration().setInt("window", args.window);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStIW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HashMapWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

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
    ToolRunner.run(new StripesPMI(), args);
  }
}
