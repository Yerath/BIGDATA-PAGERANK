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
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Retreive line .. Split it.. [0] = Node, [1] = Link;
            String line = value.toString();
            String[] tmpArray = line.split("\t");

            //Write the node and link to the reducers
            context.write(new Text(tmpArray[0]), new Text(tmpArray[1]));
        }
    }

    public static class CreateGraphReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean first = true; 
            //Add all different links to the node
            StringBuffer links = new StringBuffer();
            for (Text val : values) {
                if(first){
                    links.append(val.toString());
                    first = false;
                }else{
                    links.append("#" + val.toString());
                }
            }
            
            context.write(new Text(key.toString() + " 1.0"), new Text(links.toString()));

        }
    }

    /***************************************
     * Job 2:   Calculate PageRank
     ***************************************/

    public static class CalculatePageRankMap extends Mapper<LongWritable, Text, Text, Text>{
        private String node;
        private String node_value;
        private String[] links;
        private int total_links;

        public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException{
            //Retrieve Line {NODE VALUE  LINKS[]}
            String line = value.toString();

            //Break the line into [node value, links]
            String[] tmpArray   = line.split("\t", -1);
            String[] tmpNode    = tmpArray[0].split(" ");

            //Give values
            node        = tmpNode[0];
            node_value  = tmpNode[1];
            links       = tmpArray[1].split("#");
            total_links = links.length;

            for(int i = 0; i < links.length; i++){
                //For every link... output [LINK] [ORIGIN, VALUE, TOTAL_LINKS]
                context.write(new Text("" + links[i]), new Text("" + node + "," + node_value + "," + total_links));
            }

            context.write(new Text(node), new Text(Arrays.toString(links)));
        }
    }

    public static class CalculatePageRankReducer extends Reducer<Text, Text, Text, Text>{
        //CALCULATE NEW Value
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean first = true;
            double new_value = 0;
            String node_links = "";
            for (Text val : values) {
                //[0] == Origin, [1] == Origin currents value, [2] == Total number of links
                if(!val.toString().contains("[")){
                    String[] tmpArray = val.toString().split(",");
                    new_value = new_value + (Double.parseDouble(tmpArray[1]) / Double.parseDouble(tmpArray[2]));
                }else{
                    String tmp = val.toString()
                                    .replace("[", "")  //remove the right bracket
                                    .replace("]", "")  //remove the left bracket
                                    .replace(" ", ""); //remove space

                    StringBuffer links = new StringBuffer();
                    for (String link : tmp.split(",")) {
                        if(first){
                            links.append(link);
                            first = false;
                        }else{
                            links.append("#" + link);
                        }
                    }

                    node_links = links.toString();
                }
            }
            context.write(new Text(key.toString() + " " + new_value), new Text(node_links));
        }
    }

    /***************************************
     * Job 3:   Readable Result
     ***************************************/

    public static class ReadableResultMap extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmpArray   = value.toString().split("\t");
            String[] textValues = tmpArray[0].split(" ");
            context.write(new Text(textValues[1]), new Text(textValues[0]));
        }
    }

    public static void runJob1(String pathIn, String pathOut) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job createGraph = new Job(config);

        createGraph.setJobName("createGraphJob");
        createGraph.setJarByClass(PageRank.class);

        FileInputFormat.addInputPath(createGraph, new Path(pathIn));
        FileOutputFormat.setOutputPath(createGraph, new Path(pathOut + "_0"));

        createGraph.setMapperClass(CreateGraphMap.class);
        createGraph.setReducerClass(CreateGraphReducer.class);

        createGraph.setInputFormatClass(TextInputFormat.class);
        
        createGraph.setMapOutputKeyClass(Text.class);
        createGraph.setMapOutputValueClass(Text.class);
        
        createGraph.setOutputKeyClass(Text.class);
        createGraph.setOutputValueClass(Text.class);
        
        createGraph.waitForCompletion(true);
    }

    public static void runJob2(String pathIn, String pathOut, int iterations) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration config = new Configuration();
        
        for(int current_iteration = 0; current_iteration < iterations; current_iteration++ ){
            Job calculatePageRank = new Job(config);

            calculatePageRank.setJobName("calculatePageRank");
            calculatePageRank.setJarByClass(PageRank.class);

            FileInputFormat.addInputPath(calculatePageRank, new Path(pathIn + "_" + current_iteration));
            FileOutputFormat.setOutputPath(calculatePageRank, new Path(pathOut + "_" + (current_iteration + 1)));

            calculatePageRank.setMapperClass(CalculatePageRankMap.class);
            calculatePageRank.setReducerClass(CalculatePageRankReducer.class);

            calculatePageRank.setInputFormatClass(TextInputFormat.class);
            
            calculatePageRank.setMapOutputKeyClass(Text.class);
            calculatePageRank.setMapOutputValueClass(Text.class);
            
            calculatePageRank.setOutputKeyClass(Text.class);
            calculatePageRank.setOutputValueClass(Text.class);
            
            calculatePageRank.waitForCompletion(true);    
        }
    }

    public static void runJob3(String pathIn, String pathOut, int iterations) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration config = new Configuration();
        Job readableResult = new Job(config);

        readableResult.setJobName("calculatePageRank");
        readableResult.setJarByClass(PageRank.class);

        FileInputFormat.addInputPath(readableResult, new Path(pathIn + "_" + iterations));
        FileOutputFormat.setOutputPath(readableResult, new Path(pathOut + "_total"));

        readableResult.setMapperClass(ReadableResultMap.class);
        readableResult.setInputFormatClass(TextInputFormat.class);
        
        readableResult.setOutputKeyClass(Text.class);
        readableResult.setOutputValueClass(Text.class);
        
        readableResult.waitForCompletion(true);    
    }

    
    public static void main(String[] args) throws Exception{
        int max_interations  = 10;
        
        /*********************************************************
         * Job 1:   Create the graph
         *          This job initializes the graph with values 
         *          and the links to the nodes
         *********************************************************/
        try{
            runJob1(args[0],args[1]);
        }catch(IOException| InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }

        /*********************************************************
         * Job 2:   Calculate PageRank (Iteration)
         *          This job calculates the PageRank for every node
         *          The job will be run 10 times
         *********************************************************/
        try{
            runJob2(args[1],args[1], max_interations);
        }catch(IOException| InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }

        
        /*********************************************************
         * Job 3:   Readable Output Generator
         *          This job sorts the PageRank and makes it readable
         *********************************************************/
        try{
            runJob3(args[1],args[1], max_interations);
        }catch(IOException| InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

   
}





