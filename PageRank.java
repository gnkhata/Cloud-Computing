/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.Iterator;
import java.util.List;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * This is an example Hadoop Map/Reduce application.
 *
 * It inputs a map in adjacency list format, and performs a breadth-first search.
 * The input format is
 * ID   EDGES|DISTANCE|COLOR
 * where
 * ID = the unique identifier for a node (assumed to be an int here)
 * EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * DISTANCE = the to be determined distance of the node from the source
 * COLOR = a simple status tracking field to keep track of when we're finished with a node
 * It assumes that the source node (the node from which to start the search) has
 * been marked with distance 0 and color GRAY in the original input.  All other
 * nodes will have input distance Integer.MAX_VALUE and color WHITE.
 */
public class PageRank extends Configured implements Tool {
    public static int pass = 0;
    public static int nodeCount = 0;
    public static float alpha = 0;
    public static float danglingMass = 0;

 // public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.PageRank");

  /**
   * Nodes that are Color.WHITE or Color.BLACK are emitted, as is. For every
   * edge of a Color.GRAY node, we emit a new Node with distance incremented by
   * one. The Color.GRAY node is then colored black and is also emitted.
   */
  public static class MapClass extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {

     // LOG.info("Map executing for key [" + key.toString() + "] and value [" + value.toString()
     //     + "]");
      if(value.toString().length()>0){
          Node node = new Node(value.toString());
          if(pass == 1){
              if(node.getEdges() != null){
                  for (int v : node.getEdges()) {
                      Node vnode = new Node(v);
                      vnode.setMass(node.getMass()/node.getEdges().size());
                      output.collect(new IntWritable(vnode.getId()), vnode.getLine());
                    
                 } // end of for
              }else{
                  Node vnode = new Node(-1);
                  vnode.setMass(node.getMass());
                  output.collect(new IntWritable(vnode.getId()), vnode.getLine());
              }
              node.setMass(0);
              output.collect(new IntWritable(node.getId()), node.getLine());
        
           //   LOG.info("Map outputting for key[" + node.getId() + "] and value [" + node.getLine() + "]");
         }else{
             
             if(node.getId()== -1){
                 danglingMass = node.getMass();
             }else{
                 node.setMass((alpha/nodeCount)+((1-alpha)*node.getMass()));
                 output.collect(new IntWritable(node.getId()), node.getLine());
                 
                 Node vnode = new Node(node.getId());
                 vnode.setMass((1-alpha)*danglingMass/nodeCount);
                 output.collect(new IntWritable(vnode.getId()), vnode.getLine());
             }
         }
     }

    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
 
  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, Text> {

    /**
     * Make a new node which combines all information for this single node id.
     * The new node should have
     * - The full list of edges
     * - The minimum distance
     * - The proper Color
     */
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
     // LOG.info("Reduce executing for input key [" + key.toString() + "]");

      List<Integer> edges = null;
      float sum = 0;
      while (values.hasNext()) {
        Text value = values.next();

        Node u = new Node(key.get() + "\t" + value.toString());

        // One (and only one) copy of the node will be the fully expanded
        // version, which includes the edges
        sum = sum+u.getMass();
        if(u.getEdges() != null){
            edges = u.getEdges();
        }
      }

      Node n = new Node(key.get());
      n.setEdges(edges);
      n.setMass(sum);
      output.collect(key, new Text(n.getLine()));
     // LOG.info("Reduce outputting final key [" + key + "] and value [" + n.getLine() + "]");

    }
  }

  static int printUsage() {
    System.out.println("pagerank [-m <num mappers>] [-r <num reducers>]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  private JobConf getJobConf(String[] args) {
    JobConf conf = new JobConf(getConf(), PageRank.class);
    conf.setJobName("pagerank");

    // the keys are the unique identifiers for a Node (ints in this case).
    conf.setOutputKeyClass(IntWritable.class);
    // the values are the string representation of a Node
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapClass.class);
    conf.setReducerClass(Reduce.class);

    //conf.setNumReduceTasks(Integer.parseInt(args[0]));

    for (int i = 0; i < args.length; ++i) {
      if ("-m".equals(args[i])) {
        conf.setNumMapTasks(Integer.parseInt(args[++i]));
      } else if ("-r".equals(args[i])) {
        conf.setNumReduceTasks(Integer.parseInt(args[++i]));
      } else if ("-N".equals(args[i])){
          nodeCount = Integer.parseInt(args[++i]);
      }else if ("-alpha".equals(args[i])){
          alpha = Float.parseFloat(args[++i]);
      }
      
    }

    //LOG.info("The number of reduce tasks has been set to " + conf.getNumReduceTasks());
    //LOG.info("The number of mapper tasks has been set to " + conf.getNumMapTasks());

    return conf;
  }

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   *
   * @throws IOException
   *           When there is communication problems with the job tracker.
   */
  public int run(String[] args) throws Exception {

    int totalCount=4;

    for (int i = 0; i < args.length; ++i) {
      if ("-i".equals(args[i])) {
        totalCount= Integer.parseInt(args[++i]);
      }
    }
    int iterationCount = 0;

    while (keepGoing(iterationCount,totalCount)) {
      pass = 1;
      String input;
      if (iterationCount == 0)
        input = "input-graph";
      else
        input = "output-graph-" + iterationCount;

      String output = "output-graph-" + (iterationCount + 1+"-pass1");
      
      //First pass
      JobConf conf = getJobConf(args);
      FileInputFormat.setInputPaths(conf, new Path(input));
      FileOutputFormat.setOutputPath(conf, new Path(output));
      RunningJob job = JobClient.runJob(conf);
      
      pass = 2;
      //Second pass
      input = "output-graph-" + (iterationCount + 1+"-pass1");
      output = "output-graph-" + (iterationCount + 1);
      conf = getJobConf(args);
      FileInputFormat.setInputPaths(conf, new Path(input));
      FileOutputFormat.setOutputPath(conf, new Path(output));
      job = JobClient.runJob(conf);
      
      iterationCount++;
    }

    return 0;
  }

  private boolean keepGoing(int iterationCount, int totalCount) {
    if(iterationCount >= totalCount) {
      return false;
    }

    return true;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PageRank(), args);
    System.exit(res);
  }

}
