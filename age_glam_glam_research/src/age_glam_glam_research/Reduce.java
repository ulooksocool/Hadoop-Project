package age_glam_glam_research;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    int min = 0;
    int max = 0;
    int current = 0;
    
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter rprtr) throws IOException {
      

        while(values.hasNext()){
            current = min = max = values.next().get();
            if(current <= 0){
                continue;
            }
            break;
        }
        
        while (values.hasNext()) {
            current = values.next().get();
            if(current <= 0){
                continue;
            }
            if(current < min){
                min = current;
            }
            if(current > max){
                max = current;
            }
        }
        
        IntWritable range = new IntWritable(max - min);
        
        output.collect(key, range);
    }

}
