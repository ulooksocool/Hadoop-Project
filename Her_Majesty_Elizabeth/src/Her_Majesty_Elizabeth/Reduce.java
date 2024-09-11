package Her_Majesty_Elizabeth;

import java.io.IOException;
import java.text.DecimalFormat;
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
    private String richest,out_text;
    private long sum,max,current;
    private double mo;
    private static final DecimalFormat df = new DecimalFormat("0.00");

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {
        max = -1L;
        sum = 0L;
        count = 0;
        
        while(values.hasNext()){
            input = values.next().toString().split(",",-1);
            current = Long.parseLong(input[1]);
            
            // sum the finalworth
            sum += current;
            
            // get richest person/family/etc
            if(max < current ){
                max = current;
                richest = input[0];
            }
            
            // get count of people
            count++;
        }
        
        //get average (mean) of category
        mo = ((double) sum/count);
        String str = df.format(mo);
        str = str.replace(",",".");
        //save values to a string as category,count,sum,mo,richest
        out_text = key.toString()+ "," + count + "," + sum + "," + str + "," + richest;
        
        // save values to Text 
        out.set(out_text);

        output.collect(null, out);
    }

}
