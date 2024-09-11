package Immigration;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    
    private Text out = new Text();
    private Text country = new Text();

    private double cpi = 0;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {

        // takes each line and splits it based on ',' symbol and returns an array of each token
        String[] line = value.toString().split(",",-1);
        String str_output = "";
        cpi = 0;
        
        // make sure age exists
        if (line.length >= 25) {
            try {
                
                // validate country name
                line[5] = line[5].trim();
                if(!line[5].matches("[a-zA-Z]+[a-zA-Z ]*") || line[5].equalsIgnoreCase("country")){
                    throw new Exception("bye bye");
                }
                
                //validate decimal point number only positive range
                line[24] = line[24].trim();
                if(!line[24].matches("^[+]?[\\d]+([.]{1}[\\d]+)?$")){
                     throw new Exception("bye bye");
                }
                
                //validate integer number only positive range
                line[1] = line[1].trim();
                if(!line[1].matches("^[+]?[\\d]+$")){
                     throw new Exception("bye bye");
                }
                
                // take country name
                country.set(line[5].trim());
                
                // take country cpi value
                str_output = line[24];
                
                // take worth value
                str_output += "," + line[1];
                
                //save as Text class
                out.set(str_output);
                
                
                // emit the key as null and the age of the rich person
                output.collect(country, out);
            
            } catch (Exception e) {
                  
            }
            
        }
    }
    
}
