package Her_Majesty_Elizabeth;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private final Text out = new Text();
    private final Text category = new Text();


    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {

        // takes each line and splits it based on ',' symbol and returns an array of each token
        String[] line = value.toString().split(",", -1);
        String str_output;

        // make sure age exists
        if (line.length >= 25) {
            try {

                //validate integer number only positive range finalworth
                line[1] = line[1].trim();
                if (!line[1].matches("^[+]?[\\d]+$")) {
                    throw new Exception("bye bye");
                }

                //validate category
                line[2] = line[2].trim();
                if (!line[2].matches("[a-zA-Z]+[a-zA-Z &]*") || line[2].equalsIgnoreCase("finalworth")) {
                    throw new Exception("bye bye");
                }

                //validate name
                line[3] = line[3].trim();
                if (!line[3].matches("[a-zA-Z]+[a-zA-Z &]*") || line[3].equalsIgnoreCase("finalworth")) {
                    throw new Exception("bye bye");
                }

                // take country name
                category.set(line[2]);

                // save string name,finalworth
                str_output = line[3] + "," + line[1];
//                System.out.println("*******|Category:"+line[2]+"|name:"+line[3]+"|worth:"+line[1]+"|");
                
                //save as Text class
                out.set(str_output);

                // emit the key as null and the age of the rich person
                output.collect(category, out);

            } catch (Exception e) {

            }

        }
    }

}
