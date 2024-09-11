package Her_Majesty_Elizabeth;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private Text out = new Text();
    private Text k = new Text();
    private String[] input;
    private String out_text;
    private long sum, current;
    private int pos;
    private List<Text> list;
    private List<Text> sortedList = new ArrayList<>();
    private static final DecimalFormat df = new DecimalFormat("0.00");
    private int percent1, percent2, max;

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter rprtr) throws IOException {
        sum = 0L;

        list = getListFromIterator(values);
        list.forEach(e -> {
            input = e.toString().split(",", -1);
            current = Long.parseLong(input[2]);
            // sum the finalworth
            sum += current;
        });

        // taksinomisi analoga me pososto ploutou katigorias os pros to sinoliko plouto
        int index,num_tmp1,num_tmp2,num_max;
        for (int i = 0; i < list.size();) {
            String[] tmp1 = list.get(i).toString().split(",", -1);
            index = 0;
            max = percent1 = (int)Math.round(((double) Long.parseLong(tmp1[2]) / sum) * 100);
            num_max = num_tmp1 = Integer.parseInt(tmp1[1]);
            for (int j = i + 1; j < list.size(); j++) {
                String[] tmp2 = list.get(j).toString().split(",", -1);
                percent2 = (int)Math.round(((double) Long.parseLong(tmp2[2]) / sum) * 100);
                num_tmp2 = Integer.parseInt(tmp2[1]);
                if (percent2 > max) {
                    index = j;
                    max = percent2;
                    num_max = num_tmp2;
                }else if (percent2 == max && num_tmp2 > num_max){
                    index = j;
                    num_max = num_tmp2;
                }
            }
            // otan to pososto einai idio, taksinomisi me basi to plithos plousion ths katigorias
            if (max == percent1 && index != 0) {
                if (num_tmp1 > num_max) {
                    sortedList.add(list.get(0));
                    list.remove(0);
                } else {
                    sortedList.add(list.get(index));
                    list.remove(index);
                }
            } else {
                sortedList.add(list.get(index));
                list.remove(index);
            }

        }
        
        list.clear();
       
        sortedList.forEach( e -> {
            input = e.toString().split(",", -1);
            current = Long.parseLong(input[2]);
            pos = (int)Math.round(((double) current / sum) * 100);
            //String str = df.format(pos);
            //str = str.replace(",",".");
            out_text = pos + "%";
            for (int i = 1; i < input.length; i++) {
                out_text += " " + input[i];
            }
            out.set(out_text);
            k.set(input[0]);
            try {
                output.collect(k, out);
            } catch (IOException ex) {
                Logger.getLogger(Reduce2.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
    }

    // Function to get the List 
    public static List<Text> getListFromIterator(Iterator<Text> iterator) {
        List<Text> list = new ArrayList<>();
        while (iterator.hasNext()) {

            list.add(new Text(iterator.next().toString()));
        }
        // Return the List 
        return list;
    }


}
