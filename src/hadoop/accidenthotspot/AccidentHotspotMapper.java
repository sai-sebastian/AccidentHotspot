package hadoop.accidenthotspot;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccidentHotspotMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text location = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 1) {
            String latitude = fields[0];  // Assuming latitude is the first field
            String longitude = fields[1]; // Assuming longitude is the second field
            location.set(latitude + "," + longitude);
            context.write(location, one);
        }
    }
}
