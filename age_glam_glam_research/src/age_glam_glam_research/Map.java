package age_glam_glam_research;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    public static Text ageText = new Text("Age-Range");
    IntWritable age = new IntWritable(0);
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter rprtr) throws IOException {

        // takes each line and splits it based on ',' symbol and returns an array of each token
        String[] line = value.toString().split(",");

        // make sure age exists
        if (line.length >= 5) {
            try {
                // convert age string to int and set output value
                age = new IntWritable(Integer.parseInt(line[4]));

                // emit the key as null and the age of the rich person
                output.collect(ageText, age);
            } catch (Exception e) {
                // emit the key as null and the age of the rich person
                output.collect(ageText, age);
            }
            
        }
    }
    
}
