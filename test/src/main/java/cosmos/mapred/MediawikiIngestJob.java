package cosmos.mapred;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cosmos.impl.CosmosImpl;

/**
 * 
 */
public class MediawikiIngestJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (1 != args.length) {
      System.err.println("Usage: input.xml,input.xml,input.xml...");
      return 1;
    }
    
    String inputFiles = args[0];
    
    Job job = new Job(getConf(), "Mediawiki Ingest");
    
    job.setJarByClass(MediawikiIngestJob.class);
    
    Configuration conf = job.getConfiguration();
    
    String tablename = "sortswiki";
    String zookeepers = "localhost:2181";
    String instanceName = "accumulo1.5";
    String user = "mediawiki";
    String passwd = "password";
    
    //conf.set("io.file.buffer.size", Integer.toString(64*1024*1024));
    FileInputFormat.setInputPaths(job, inputFiles);
    //FileInputFormat.setMinInputSplitSize(job, 1024*1024*100);
    
    job.setMapperClass(MediawikiMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    
    job.setInputFormatClass(MediawikiInputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
    AccumuloOutputFormat.setOutputInfo(conf, user, passwd.getBytes(), true, tablename);
    AccumuloOutputFormat.setMaxMutationBufferSize(conf, CosmosImpl.DEFAULT_MAX_MEMORY);
    AccumuloOutputFormat.setMaxLatency(conf, CosmosImpl.DEFAULT_MAX_LATENCY.intValue());
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /*
  private List<Path> getInputPaths(String pathArg) {
    Iterable<String> strPaths = Splitter.on(',').split(pathArg);
    
    return Lists.newArrayList(Iterables.transform(strPaths, new Function<String,Path>() {

      @Override
      public Path apply(String input) {
        return new Path(input);
      }
      
    }));
  }*/
 

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MediawikiIngestJob(), args);
    System.exit(res);
  }
  
}
