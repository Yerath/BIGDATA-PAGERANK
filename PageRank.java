package nl.hu.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRank{
    public float default_value  = 0;
    public float jump           = 0;
    
    /***************************************
     * Job 1:   Create the graph
     ***************************************/
    public static class CreateGraphMap extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
            //Retreive line .. Split it.. [0] = Node, [1] = Link;
            String line = value.toString();
            String[] tmpArray = line.split(" ");

            System.out.println(tmpArray.toString());

            //Write the node and link to the reducers
            context.write(new Text(tmpArray[0]), new Text(tmpArray[1]));
        }
    }

    public static class CreateGraphReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            //Add all different links to the node
            StringBuffer links = new StringBuffer();
            for (Text val : values) {
                links.append(val.toString());
            }

            System.out.println(key.toString() + " 1.0 "+ links.toString());
            context.write(new Text(key.toString() + " 1.0"), new Text(links.toString()));

        }
    }

    /***************************************
     * Job 2:   Calculate PageRank
     ***************************************/


    
    public static void main(String[] args) throws Exception{
        /*********************************************************
         * Job 1:   Create the graph
         *          This job initializes the graph with values 
         *          and the links to the nodes
         *********************************************************/
        Configuration config = new Configuration();
        Job createGraph = new Job(config);

        createGraph.setJobName("createGraphJob");
        createGraph.setJarByClass(PageRank.class);

        FileInputFormat.addInputPath(createGraph, new Path(args[0]));
        FileOutputFormat.setOutputPath(createGraph, new Path(args[1]));

        createGraph.setMapperClass(CreateGraphMap.class);
        createGraph.setReducerClass(CreateGraphReducer.class);

        createGraph.setInputFormatClass(TextInputFormat.class);
        
        createGraph.setMapOutputKeyClass(Text.class);
        createGraph.setMapOutputValueClass(Text.class);
        
        createGraph.setOutputKeyClass(Text.class);
        createGraph.setOutputValueClass(Text.class);
        
        createGraph.waitForCompletion(true);
        

        /*********************************************************
         * Job 2:   Calculate PageRank (Iteration)
         *          This job calculates the PageRank for every node
         *          The job will be run 10 times
         *********************************************************/



        /*********************************************************
         * Job 3:   Readable Output Generator
         *          This job sorts the PageRank and makes it readable
         *********************************************************/
    }
}





