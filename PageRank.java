package nl.hu.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class PageRank{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
        	//Get the string line
        	String line = value.toString();

            //New Node, Amount
            output.collect(new Text(""), new Text(ListToText(users)));
            
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
            //Create an temporary array that can store 2 lists.. Every combination appears twice.
            String[] friends = new String[2];
            
            //Fill the friends array
            for (int i = 0; i < 2; i++)
            	friends[i] = values.next().toString();

            //Split them into different lists
            String[] list1 = friends[0].split(" ");
            String[] list2 = friends[1].split(" ");

            //Filter out the duplicates 
            List<String> list = new LinkedList<String>();
            for(String friend1 : list1){
                for(String friend2 : list2){
                    if(friend1.equals(friend2)){
                        list.add(friend1);
                    }
                }
            }

            //Add them to an stringbuffer
            StringBuffer sb = new StringBuffer();
            for(int i = 0; i < list.size(); i++){
                    sb.append(list.get(i));
                    if(i != list.size() - 1)
                            sb.append(" ");
            }
            
            output.collect(key, new Text(sb.toString()));
        }
    }

	public static void main(String[] args) throws Exception{
		    JobConf conf = new JobConf(CommonFriends.class);
            conf.setJobName("Friend");
 
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
 
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);
 
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
 
            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
            JobClient.runJob(conf);
    }	
}





