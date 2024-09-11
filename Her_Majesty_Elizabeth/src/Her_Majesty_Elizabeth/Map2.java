package Her_Majesty_Elizabeth;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Text k;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {
        k = new Text("no");
        output.collect(k, value);
    }

}
