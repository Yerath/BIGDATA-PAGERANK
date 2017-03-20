package nl.hu.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Scanner;
 
import org.apache.commons.lang.StringUtils;
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
    public static double jump = 0.15;
    public static LinkedHashSet<String> dangeling_nodes = new LinkedHashSet<String>();
    
    /***************************************
     * Job 0:   Remove Dangling Nodes
     ***************************************/

    public static class RemoveDanglingNodesReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Add all different links to the node
            StringBuffer links = new StringBuffer();
            for (Text val : values) {
                if(!val.toString().isEmpty()){
                    if(first){
                        links.append(val.toString());
                        first = false;
                    }else{
                        links.append("#" + val.toString());
                    }
                }
            }

            if(links.toString().isEmpty()){
                System.out.println("Found an Dangling Node: " + key.toString());
                dangeling_nodes.add(key.toString());
            }else{
                String[] tmpArray = links.toString().split("#");
                for(int i = 0; i < tmpArray.length; i++){
                    context.write(new Text(key.toString()), new Text(tmpArray[i]));
                }
            }
        }
    }

    /***************************************
     * Job 1:   Create the graph
     ***************************************/
    public static class CreateGraphMap extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmpArray;
            String line = value.toString();
            if(line.contains("\t")){
                tmpArray = line.split("\t");
            }else{
                tmpArray = line.split(" ");
            }

            //Write the Node and Link
            if(!dangeling_nodes.contains(tmpArray[0]) && !dangeling_nodes.contains(tmpArray[1]) ){
                context.write(new Text(tmpArray[0]), new Text(tmpArray[1]));
                context.write(new Text(tmpArray[1]), new Text(""));
            }
        }
    }

    public static class CreateGraphReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean first = true; 
            //Add all different links to the node
            StringBuffer links = new StringBuffer();
            for (Text val : values) {
                if(!val.toString().isEmpty()){
                    if(first){
                        links.append(val.toString());
                        first = false;
                    }else{
                        links.append("#" + val.toString());
                    }
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

            //Somehow check of er geen links zijn.
            String line = value.toString();

            //Break the line into [node value, links]
            String[] tmpArray   = line.split("\t", -1);
            String[] tmpNode    = tmpArray[0].split(" ");

            //Give values
            node        = tmpNode[0];
            node_value  = tmpNode[1];

            List<String> links = new LinkedList<String>(Arrays.asList(tmpArray[1].split("#")));
        
            total_links = links.size();
            if(total_links != 0){
                for(int i = 0; i < links.size(); i++){
                    if(!dangeling_nodes.contains( links.get(i) ) ){
                        context.write(new Text("" + links.get(i)), new Text("" + node + "," + node_value + "," + total_links));
                    }
                }
            }
            context.write(new Text(node), new Text(links.toString()));
            
        }
    }

    public static class CalculatePageRankReducer extends Reducer<Text, Text, Text, Text>{
        //CALCULATE NEW Value
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean first = true;
            double new_value = 0;
            String node_links = "";

            for (Text val : values) {
                if(!val.toString().contains("[")){
                    String[] tmpArray = val.toString().split(",");
                    new_value = new_value + (Double.parseDouble(tmpArray[1]) / Double.parseDouble(tmpArray[2]));
                }else{

                    String tmp = val.toString()
                                    .replace("[", "")  //remove the right bracket
                                    .replace("]", "")  //remove the left bracket
                                    .replace(" ", ""); //remove space

                    //Original Link list
                    List<String> links = new LinkedList<String>(Arrays.asList(tmp.split(",")));

                    //Check for links to an dangeling node
                    for (int i = 0; i < links.size(); i++) {
                        if(dangeling_nodes.contains(links.get(i))){
                            links.remove(i);
                        }
                    }

                    node_links = StringUtils.join(links.toArray(),"#");
                }
            }

            new_value = (1-jump) + (jump * new_value);
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

    public static void runJob0(String pathIn, String pathOut, int iteration) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job removeDanglingNodes = new Job(config);

        removeDanglingNodes.setJobName("createGraphJob");
        removeDanglingNodes.setJarByClass(PageRank.class);

        if(iteration == 0){
            FileInputFormat.addInputPath(removeDanglingNodes, new Path(pathIn));
        }else{
            FileInputFormat.addInputPath(removeDanglingNodes, new Path(pathOut + "_cleaning_" + (iteration)));   
        }
        FileOutputFormat.setOutputPath(removeDanglingNodes, new Path(pathOut + "_cleaning_" + (iteration + 1)));

        removeDanglingNodes.setMapperClass(CreateGraphMap.class);
        removeDanglingNodes.setReducerClass(RemoveDanglingNodesReducer.class);

        removeDanglingNodes.setInputFormatClass(TextInputFormat.class);
        
        removeDanglingNodes.setMapOutputKeyClass(Text.class);
        removeDanglingNodes.setMapOutputValueClass(Text.class);
        
        removeDanglingNodes.setOutputKeyClass(Text.class);
        removeDanglingNodes.setOutputValueClass(Text.class);
        
        removeDanglingNodes.waitForCompletion(true);
    }

    public static void runJob1(String pathIn, String pathOut, int cleaning) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = new Configuration();
        Job createGraph = new Job(config);

        createGraph.setJobName("createGraphJob");
        createGraph.setJarByClass(PageRank.class);

        FileInputFormat.addInputPath(createGraph, new Path(pathOut + "_cleaning_" + cleaning));
        FileOutputFormat.setOutputPath(createGraph, new Path(pathOut + "_result_0"));

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

            FileInputFormat.addInputPath(calculatePageRank, new Path(pathIn + "_result_" + current_iteration));
            FileOutputFormat.setOutputPath(calculatePageRank, new Path(pathOut + "_result_" + (current_iteration + 1)));

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

        FileInputFormat.addInputPath(readableResult, new Path(pathIn + "_result_" + iterations));
        FileOutputFormat.setOutputPath(readableResult, new Path(pathOut + "_total"));

        readableResult.setMapperClass(ReadableResultMap.class);
        readableResult.setInputFormatClass(TextInputFormat.class);
        
        readableResult.setOutputKeyClass(Text.class);
        readableResult.setOutputValueClass(Text.class);
        
        readableResult.waitForCompletion(true);    
    }

    
    public static void main(String[] args) throws Exception{
        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.print("Enter amount of iterations: ");
        int max_interations = reader.nextInt(); // Scans the next token of the input as an int.

        reader = new Scanner(System.in);  // Reading from System.in
        System.out.print("Enter the jump factor (Normally 0.15): ");
        jump = reader.nextDouble(); // Scans the next token of the input as an int.
        
        System.out.println();

        System.out.println("Iterations set to : " + max_interations);
        System.out.println("Jump factor set to : " + jump);

        System.out.println("/*********************************************************");
        System.out.println(" * Job 0:   Remove Dangling Nodes ");
        System.out.println(" *          This job removes Dangling Nodes from the input file");
        System.out.println(" *          Runs till there are no Dangling Nodes ");
        System.out.println("*********************************************************/");
        int iteration = 0;
        while(iteration < 20){
            try{
                runJob0(args[0],args[1], iteration);
            }catch(IOException| InterruptedException | ClassNotFoundException e){
                e.printStackTrace();
            }
            System.out.println("BOOLEAN: " + found_dangling_nodes);
            iteration = iteration + 1;
        }
        
        System.out.println("/*********************************************************");
        System.out.println(" * Job 1:   Create the graph ");
        System.out.println(" *          This job initializes the graph with values ");
        System.out.println(" *          and the links to the nodes ");
        System.out.println("*********************************************************/");

        try{
            runJob1(args[0],args[1],iteration);
        }catch(IOException| InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }

        System.out.println("/*********************************************************");
        System.out.println(" * Job 2:   Calculate PageRank (Iteration) ");
        System.out.println(" *          This job calculates the PageRank for every node ");
        System.out.println(" *          The job will be run 10 times ");
        System.out.println("*********************************************************/");
        
        try{
            runJob2(args[1],args[1], max_interations);
        }catch(IOException| InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }

        System.out.println("/*********************************************************");
        System.out.println(" * Job 3:   Readable Output Generator ");
        System.out.println(" *          This job sorts the PageRank and makes it readable ");
        System.out.println("*********************************************************/");

        try{
            runJob3(args[1],args[1], max_interations);
        }catch(IOException| InterruptedException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

   
}





