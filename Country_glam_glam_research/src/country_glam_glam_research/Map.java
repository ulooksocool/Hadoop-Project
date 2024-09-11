package country_glam_glam_research;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text country;
    private static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter rprtr) throws IOException {

        // takes each line and splits it based on ',' symbol and returns an array of each token
        String[] line = value.toString().split(",",-1);

        // make sure country exists
        if (line.length >= 6) {
            if(line[5].matches("[a-zA-Z]+[a-zA-Z ]*") && !line[5].equalsIgnoreCase("country")){
                country = new Text(line[5]);
                output.collect(country, one);
            }
        }
    }

}
