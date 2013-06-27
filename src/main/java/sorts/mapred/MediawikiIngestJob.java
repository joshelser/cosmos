package sorts.mapred;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 */
public class MediawikiIngestJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf(), "Mediawiki Ingest");
    
    job.setJarByClass(MediawikiIngestJob.class);
    
    Configuration conf = job.getConfiguration();
    
    String tablename = "sortswiki";
    String zookeepers = "localhost:2181";
    String instanceName = "accumulo1.5";
    String user = "mediawiki";
    PasswordToken passwd = new PasswordToken("password");    
    
    FileInputFormat.setInputPaths(job, new Path("/enwiki-20111201-pages-articles.xml"));
    
    job.setMapperClass(MediawikiMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    
    job.setInputFormatClass(MediawikiInputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zookeepers);
    AccumuloOutputFormat.setConnectorInfo(job, user, passwd);
    AccumuloOutputFormat.setBatchWriterOptions(job, bwConfig);
    AccumuloOutputFormat.setCreateTables(job, true);
    AccumuloOutputFormat.setDefaultTableName(job, tablename);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
 

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MediawikiIngestJob(), args);
    System.exit(res);
  }
  
}
