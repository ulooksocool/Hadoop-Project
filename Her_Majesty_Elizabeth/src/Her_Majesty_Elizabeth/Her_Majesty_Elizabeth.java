package Her_Majesty_Elizabeth;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Her_Majesty_Elizabeth {

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Her_Majesty_Elizabeth.class);
        conf.setJobName("Eixes kai sto xorio soy xrisi lekani part 1");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf).waitForCompletion();

        //second job
        JobConf conf2 = new JobConf(Her_Majesty_Elizabeth.class);
        conf2.setJobName("Eixes kai sto xorio soy xrisi lekani part 2");

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);

        conf2.setMapperClass(Map2.class);
        conf2.setReducerClass(Reduce2.class);

        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, new Path(args[1] + "/part-00000"));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1] + "/final"));

        JobClient.runJob(conf2);
    }

}
