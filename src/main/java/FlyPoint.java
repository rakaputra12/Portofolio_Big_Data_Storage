import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;

public class FlyPoint {
    public static class PointsWritable implements Writable {

        private final IntWritable pointsEarned = new IntWritable();
        private final  IntWritable pointsRedeemed = new IntWritable();
        public void setPointsEarned(int pointsEarned){
            this.pointsEarned.set(pointsEarned);
        }
        public void setPointsRedeemed(int pointsRedeemed){
            this.pointsRedeemed.set(pointsRedeemed);
        }

        public int getPointsEarned() {
            return pointsEarned.get();
        }

        public int getPointsRedeemed() {
            return pointsRedeemed.get();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            pointsEarned.write(out);
            pointsRedeemed.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            pointsEarned.readFields(in);
            pointsRedeemed.readFields(in);
        }

        @Override
        public String toString() {
            return pointsEarned.toString() + " " + pointsRedeemed.toString();
        }
    }


    public static class PointsMapper extends Mapper<Object, Text, Text, PointsWritable> {

        private final PointsWritable valueOut = new PointsWritable();
        private final Text customerId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (!parts[0].equals("Customer")) {
                // Extract relevant fields
                String customerIdStr = parts[0];
                int pointsEarned = (int) Double.parseDouble(parts[7]); // Parse as double and then cast to int
                int pointsRedeemed = (int) Double.parseDouble(parts[8]); // Parse as double and then cast to int


                // Set values to PointsWritable object
                valueOut.setPointsEarned(pointsEarned);
                valueOut.setPointsRedeemed(pointsRedeemed);

                // Emit key-value pair (customer ID, PointsWritable)
                customerId.set(customerIdStr);
                context.write(customerId, valueOut);
            }
        }
    }


    public static class PointsCombiner extends Reducer<Text, PointsWritable, Text, PointsWritable> {

        private final PointsWritable result = new PointsWritable();


        public void reduce(Text key, Iterable<PointsWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            for (PointsWritable points : values) {
                totalPointsEarned += points.getPointsEarned();
                totalPointsRedeemed += points.getPointsRedeemed();
            }

            result.setPointsEarned(totalPointsEarned);
            result.setPointsRedeemed(totalPointsRedeemed);
            context.write(key,result);
        }
    }

    public static class PointsReducer extends Reducer<Text, PointsWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<PointsWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            for (PointsWritable points : values) {
                totalPointsEarned += points.getPointsEarned();
                totalPointsRedeemed += points.getPointsRedeemed();
            }
            result.set(Integer.toString(totalPointsEarned) + " " + Integer.toString(totalPointsRedeemed));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        final Path outPath = new Path("output");
        final FileContext fc = FileContext.getFileContext();

        fc.delete(outPath, true);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fly Points Calculator");
        job.setJarByClass(FlyPoint.class);
        job.setMapperClass(PointsMapper.class);
        job.setMapOutputValueClass(PointsWritable.class);
        job.setCombinerClass(PointsCombiner.class);
        job.setReducerClass(PointsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PointsWritable.class);

        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
