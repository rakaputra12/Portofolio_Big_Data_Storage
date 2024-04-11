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

    // Klasse für die Gruppenpunkte als Writable
    public static class PointsGroup implements Writable {
        // Variablen für verdiente Punkte, eingelöste Punkte und Anzahl der Kunden in der Gruppe
        private final IntWritable pointsEarned = new IntWritable();
        private final IntWritable pointsRedeemed = new IntWritable();
        private final IntWritable customerCount = new IntWritable();

        // Setze verdiente Punkte
        public void setPointsEarned(int pointsEarned) {
            this.pointsEarned.set(pointsEarned);
        }

        // Setze eingelöste Punkte
        public void setPointsRedeemed(int pointsRedeemed) {
            this.pointsRedeemed.set(pointsRedeemed);
        }

        // Setze Anzahl der Kunden
        public void setCustomerCount(int customerCount) {
            this.customerCount.set(customerCount);
        }

        // Holen der verdienten Punkte
        public int getPointsEarned() {
            return pointsEarned.get();
        }

        // Holen der eingelösten Punkte
        public int getPointsRedeemed() {
            return pointsRedeemed.get();
        }

        // Holen der Anzahl der Kunden
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

        // Konvertiere die Werte in einen String
        @Override
        public String toString() {
            return pointsEarned.toString() + " " + pointsRedeemed.toString() + " " + customerCount.toString();
        }
    }

    // Mapper-Klasse für die Gruppierung von Punkten
    public static class PointsMapper extends Mapper<Object, Text, Text, PointsGroup> {
        // Schlüssel für die Gruppenzuordnung
        private final Text groupKey = new Text();

        // Objekt für die Gruppenpunkte
        private final PointsGroup group = new PointsGroup();

        // Map-Funktion
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Aufteilen der Eingabezeile anhand von Leerzeichen
            String[] parts = value.toString().split("\\s+");
            // Extrahieren der verdienten und eingelösten Punkte
            int pointsEarned = Integer.parseInt(parts[1]);
            int pointsRedeemed = Integer.parseInt(parts[2]);

            // Bestimmung der Gruppe basierend auf den verdienten Punkten
            if (pointsEarned <= 2000) {
                groupKey.set("Group1");
            } else if (pointsEarned <= 10000) {
                groupKey.set("Group2");
            } else {
                groupKey.set("Group3");
            }

            // Setzen der Punkte in das Gruppenobjekt
            group.setPointsEarned(pointsEarned);
            group.setPointsRedeemed(pointsRedeemed);

            // Emission des Schlüssel-Wert-Paares (Gruppe, Gruppenobjekt)
            context.write(groupKey, group);
        }
    }

    // Combiner-Klasse für die Gruppenpunkte
    public static class PointsCombiner extends Reducer<Text, PointsGroup, Text, PointsGroup> {
        // Ergebnisobjekt für die kombinierten Gruppenpunkte
        private final PointsGroup result = new PointsGroup();

        // Reduce-Funktion
        public void reduce(Text key, Iterable<PointsGroup> values, Context context)
                throws IOException, InterruptedException {
            // Initialisierung der Variablen für die Gesamtpunkte und die Anzahl der Kunden
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            int customerCount = 0;

            // Berechnung der Gesamtpunkte und der Anzahl der Kunden für die Gruppe
            for (PointsGroup group : values) {
                totalPointsEarned += group.getPointsEarned();
                totalPointsRedeemed += group.getPointsRedeemed();
                customerCount++;
            }

            // Setzen der kombinierten Werte im Ergebnisobjekt
            result.setPointsEarned(totalPointsEarned);
            result.setPointsRedeemed(totalPointsRedeemed);
            result.setCustomerCount(customerCount);

            // Emission des Schlüssel-Wert-Paares (Gruppe, kombiniertes Ergebnisobjekt)
            context.write(key, result);
        }
    }

    // Reducer-Klasse für die Gruppenpunkte
    public static class PointsReducer extends Reducer<Text, PointsGroup, Text, Text> {
        // Ergebnisobjekt für die kombinierten Gruppenpunkte
        private final Text result = new Text();

        // Reduce-Funktion
        public void reduce(Text key, Iterable<PointsGroup> values, Context context)
                throws IOException, InterruptedException {
            // Initialisierung der Variablen für die Gesamtpunkte und die Anzahl der Kunden
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            int customerCount = 0;

            // Abrufen der kombinierten Ergebnisse aus dem Combiner
            for (PointsGroup group : values) {
                totalPointsEarned += group.getPointsEarned();
                totalPointsRedeemed += group.getPointsRedeemed();
                customerCount += group.getCustomerCount();
            }

            // Berechnung des Einlösungsprozentsatzes
            double redemptionPercentage = (double) totalPointsRedeemed / totalPointsEarned * 100;

            // Formatieren und Setzen des Ergebnisstrings
            result.set(customerCount + " " + totalPointsEarned + " " + String.format("%.0f", redemptionPercentage) + "%");

            // Emission des Schlüssel-Wert-Paares (Gruppe, Ergebnisstring)
            context.write(key, result);
        }
    }


    // Hauptmethode
    public static void main(String[] args) throws Exception {
        // Festlegen des Ausgabepfads für die Ergebnisse des Jobs
        final Path outPath = new Path("output2");
        // Zugriff auf das Dateisystem für das Löschen des Ausgabeverzeichnisses (falls vorhanden)
        final FileContext fc = FileContext.getFileContext();

        // Löschen des Ausgabeverzeichnisses, um sicherzustellen, dass es leer ist
        fc.delete(outPath, true);

        // Konfiguration des MapReduce-Jobs
        Configuration conf = new Configuration();
        // Erstellen einer neuen Job-Instanz mit der angegebenen Konfiguration und einem Namen für den Job
        Job job = Job.getInstance(conf, "Fly Group Points Calculator");
        // Setzen der Klasse, die den ausführbaren Jar-Dateipfad enthält, für den MapReduce-Job
        job.setJarByClass(FlyPointsGroup.class);
        // Setzen der Mapper-Klasse für den Job
        job.setMapperClass(PointsMapper.class);
        // Setzen des Datentyps für die Ausgabe des Mappers
        job.setMapOutputValueClass(PointsGroup.class);
        // Setzen der Combiner-Klasse für die lokale Aggregation der Mapper-Ausgabe
        job.setCombinerClass(PointsCombiner.class);
        // Setzen der Reducer-Klasse für den Job
        job.setReducerClass(PointsReducer.class);
        // Setzen des Datentyps für den Ausgabeschlüssel des Jobs
        job.setOutputKeyClass(Text.class);
        // Setzen des Datentyps für den Ausgabewert des Jobs
        job.setOutputValueClass(Text.class); // PointsGroup.class works also

        // Hinzufügen des Eingabeverzeichnisses zum MapReduce-Job
        FileInputFormat.addInputPath(job, new Path("output"));
        // Festlegen des Ausgabeverzeichnisses für die Ergebnisse des Jobs
        FileOutputFormat.setOutputPath(job, outPath);

        // Starten des MapReduce-Jobs und Warten auf dessen Abschluss
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
