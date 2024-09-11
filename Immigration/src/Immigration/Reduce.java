package Immigration;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private Text out = new Text();
    private int count = 0;
    private String[] input;
    private String cpi;
    private long fworth;
    

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {

        count = 0;
        while(values.hasNext()){
            input = values.next().toString().split(",",-1);
            cpi = input[0];
            fworth += Long.parseLong(input[1]);
            count++;
        }
        out.set(cpi + " " + fworth + " " + count);
         
        output.collect(key, out);
    }

}
