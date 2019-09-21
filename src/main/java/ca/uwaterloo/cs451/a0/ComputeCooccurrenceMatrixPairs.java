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

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfFloats;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;


import java.io.File;
import java.util.Scanner;
import java.lang.Math;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;


































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

//---------------------------------------------------------------------
//---------------------------------------------------------------------
public class ComputeCooccurrenceMatrixPairs extends Configured implements Tool {	
	
  private static final Logger LOG = Logger.getLogger(ComputeCooccurrenceMatrixPairs.class);
	public static int total = 0;
	
	
	public static final class MyMapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      for (String word : Tokenizer.tokenize(value.toString())) {
        WORD.set(word);
        context.write(WORD, ONE);
      }
    }
  }

  // Reducer: sums up all the counts.
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
	    total += sum;
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
      List<String> tokens = Tokenizer.tokenize(value.toString());
	int size = tokens.size();
	if(size > 40){
		size = 40;
	}
      for (int i = 0; i < tokens.size(); i++) {
        for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.size()); j++) {
          if (i == j) continue;
          PAIR.set(tokens.get(i), tokens.get(j));
          context.write(PAIR, ONE);
        }
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStrings, PairOfFloats, PairOfStrings, PairOfFloats> {
    private static final PairOfFloats SUM = new PairOfFloats();
	  
	  public int giveCount(String word)
	  {
		  try{
			  File file = new File("temp/part-r-00000");
			  Scanner sc = new Scanner(file);
		  while (sc.hasNextLine())
                {
                        String temp = sc.nextLine();
                  
                        String yo = "";
                String[] arrOfStr = temp.split("\t");

                if(arrOfStr[0].equals(word))
			return(Integer.parseInt(arrOfStr[1]));
		  }
		  }
		  catch(Exception e)
		  {
			  System.out.println("LOL No File Found");
		  }
		  
		  return(0);
	  }

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloats> values, Context context)
        throws IOException, InterruptedException {
	int threshold = 0;
	double pmi=1.0;
	float pmi_2f = 1;
	threshold = context.getConfiguration().getInt("threshold",3);
      Iterator<PairOfFloats> iter = values.iterator();
      float sum = 0;
      while (iter.hasNext()) {
        sum = sum +  iter.next().getLeftElement();
      }
	if(sum > threshold){
		String x = key.getLeftElement();
		String y = key.getRightElement();
		int xCount = giveCount(x);
		int yCount = giveCount(y);
		pmi = ((total * sum) / (xCount * yCount));  //read from file to determine number of x and y
		pmi = (Math.log10(pmi));
		pmi_2f = (float)pmi;
		
      SUM.set(sum,pmi_2f);
      context.write(key, SUM);
	}
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
	

/**
Word Count Implementation
*/

	

  /**
   * Creates an instance of this tool.
   */
  private ComputeCooccurrenceMatrixPairs() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;
    
    @Option(name = "-threshold", metaVar = "[num]", usage = "minimum threshold")
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

//     LOG.info("Tool: " + ComputeCooccurrenceMatrixPairs.class.getSimpleName());
//     LOG.info(" - input path: " + args.input);
//     LOG.info(" - output path: " + args.output);
//     LOG.info(" - window: " + args.window);
//     LOG.info(" - number of reducers: " + args.numReducers);
//     LOG.info(" - minimum threshold: " + args.threshold);
	  
	  
	  
	  
	  
	  
    Configuration conf = getConf();
    Job jobWordCount = Job.getInstance(conf);
    jobWordCount.setJobName(ComputeCooccurrenceMatrixPairs.class.getSimpleName());
    jobWordCount.setJarByClass(ComputeCooccurrenceMatrixPairs.class); //=========================================

    jobWordCount.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(jobWordCount, new Path(args.input));
    FileOutputFormat.setOutputPath(jobWordCount, new Path(args.output));

    jobWordCount.setMapOutputKeyClass(Text.class);
    jobWordCount.setMapOutputValueClass(IntWritable.class);
    jobWordCount.setOutputKeyClass(Text.class);
    jobWordCount.setOutputValueClass(IntWritable.class);
    jobWordCount.setOutputFormatClass(TextOutputFormat.class);

    jobWordCount.setMapperClass(MyMapperWordCount.class);
    jobWordCount.setCombinerClass(MyReducerWordCount.class);
    jobWordCount.setReducerClass(MyReducerWordCount.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path("temp");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    jobWordCount.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

	  
	  
	  
	  
	  
	  

    Job job = Job.getInstance(getConf());
    job.setJobName(ComputeCooccurrenceMatrixPairs.class.getSimpleName());
    job.setJarByClass(ComputeCooccurrenceMatrixPairs.class);

    Path outputDir1 = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir1, true);

    job.getConfiguration().setInt("window", args.window);
    job.getConfiguration().setInt("threshold", args.threshold);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ComputeCooccurrenceMatrixPairs(), args);

  }
}
