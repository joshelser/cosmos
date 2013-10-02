/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.mapred;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
    
    Configuration conf = getConf();
    System.out.println( "path " + conf.get("fs.default.name") );
    conf.addResource(new Path("/opt/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
    
    conf.addResource(new Path("/opt/hadoop/conf/mapred-site.xml"));
    
    System.out.println( "path " + conf.get("fs.default.name") );
    //System.exit(1);
    Job job = new Job(conf, "Mediawiki Ingest");
    
    
    
    job.setJarByClass(MediawikiIngestJob.class);
    
    String tablename = "sortswiki";
    String zookeepers = "localhost:2181";
    String instanceName = "accumulo";
    String user = "root";
    String passwd = "password";
    
    FileInputFormat.setInputPaths(job, inputFiles);
    
    job.setMapperClass(MediawikiMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    
    job.setInputFormatClass(MediawikiInputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
    AccumuloOutputFormat.setOutputInfo(conf, user, passwd.getBytes(), true, tablename);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
 

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MediawikiIngestJob(), args);
    System.exit(res);
  }
  
}
