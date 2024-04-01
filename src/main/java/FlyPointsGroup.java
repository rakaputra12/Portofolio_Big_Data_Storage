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


public class FlyPointsGroup {

    public static class PointsGroup implements Writable{

        private final IntWritable pointsEarned = new IntWritable();
        private final IntWritable pointsRedeemed = new IntWritable();
        private final IntWritable customerCount = new IntWritable();


        public void setPointsEarned(int pointsEarned) {
            this.pointsEarned.set(pointsEarned);
        }

        public void setPointsRedeemed(int pointsRedeemed) {
            this.pointsRedeemed.set(pointsRedeemed);
        }

        public void setCustomerCount(int customerCount) {
            this.customerCount.set(customerCount);
        }
        public int getPointsEarned() {
            return pointsEarned.get();
        }

        public int getPointsRedeemed() {
            return pointsRedeemed.get();
        }
        public int getCustomerCount() {
            return customerCount.get();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            pointsEarned.write(out);
            pointsRedeemed.write(out);
            customerCount.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            pointsEarned.readFields(in);
            pointsRedeemed.readFields(in);
            customerCount.readFields(in);
        }

        @Override
        public String toString() {
            return pointsEarned.toString() + " " + pointsRedeemed.toString() + " " + customerCount.toString();
        }



    }

    public static class PointsMapper extends Mapper<Object, Text, Text, PointsGroup> {
        private final Text groupKey = new Text();

        private final PointsGroup group = new PointsGroup();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+"); // Splitting by one or more whitespace characters
            int pointsEarned = Integer.parseInt(parts[1]); // Points earned is at index 1
            int pointsRedeemed = Integer.parseInt(parts[2]); // Points redeemed is at index 2

            if (pointsEarned <= 2000) {
                groupKey.set("Group1");
            } else if (pointsEarned <= 10000) {
                groupKey.set("Group2");
            } else {
                groupKey.set("Group3");
            }

            group.setPointsEarned(pointsEarned);
            group.setPointsRedeemed(pointsRedeemed);

            context.write(groupKey, group);
          }
    }

    public static class PointsCombiner extends Reducer<Text, PointsGroup, Text, PointsGroup> {
        private final PointsGroup result = new PointsGroup();

        public void reduce(Text key, Iterable<PointsGroup> values, Context context)
                throws IOException, InterruptedException {
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            int customerCount = 0;
            // Berechnung der Anzahl der Kunden und deren Punkte
            for (PointsGroup group : values) {
                totalPointsEarned += group.getPointsEarned();
                totalPointsRedeemed += group.getPointsRedeemed();
                customerCount++;
            }

            result.setPointsEarned(totalPointsEarned);
            result.setPointsRedeemed(totalPointsRedeemed);
            result.setCustomerCount(customerCount);
            context.write(key, result);
        }
    }
    public static class PointsReducer extends Reducer<Text, PointsGroup, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<PointsGroup> values, Context context)
                throws IOException, InterruptedException {
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            int customerCount = 0;
            // Abruf der Ergebnisse aus Combiner
            for (PointsGroup group : values) {
                totalPointsEarned += group.getPointsEarned();
                totalPointsRedeemed += group.getPointsRedeemed();
                customerCount += group.getCustomerCount();
            }

            double redemptionPercentage = (double) totalPointsRedeemed / totalPointsEarned * 100;
            result.set(customerCount + " " + totalPointsEarned + " " + String.format("%.0f", redemptionPercentage) + "%");
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        final Path outPath = new Path("output2");
        final FileContext fc = FileContext.getFileContext();

        fc.delete(outPath, true);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fly Group Points Calculator");
        job.setJarByClass(FlyPoint.class);
        job.setMapperClass(PointsMapper.class);
        job.setMapOutputValueClass(PointsGroup.class);
        job.setCombinerClass(PointsCombiner.class);
        job.setReducerClass(PointsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PointsGroup.class);

        FileInputFormat.addInputPath(job, new Path("output"));
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
