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

    // Klasse für das Schreiben und Lesen von Punkten als Hadoop-Writable
    public static class PointsWritable implements Writable {

        public final IntWritable pointsEarned = new IntWritable();
        public final IntWritable pointsRedeemed = new IntWritable();

        // Setze verdiente Punkte
        public void setPointsEarned(int pointsEarned) {
            this.pointsEarned.set(pointsEarned);
        }

        // Setze eingelöste Punkte
        public void setPointsRedeemed(int pointsRedeemed) {
            this.pointsRedeemed.set(pointsRedeemed);
        }

        // Holen der verdienten Punkte
        public int getPointsEarned() {
            return pointsEarned.get();
        }

        // Holen der eingelösten Punkte
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

        // Konvertiere die Punkte in einen String
        @Override
        public String toString() {
            return pointsEarned.toString() + " " + pointsRedeemed.toString();
        }
    }

    // Mapper-Klasse
    public static class PointsMapper extends Mapper<Object, Text, Text, PointsWritable> {
        // Initialisierung von Objekten für den Wert und die Kunden-ID
        private final PointsWritable valueOut = new PointsWritable();
        private final Text customerId = new Text();

        // Map-Funktion
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Aufteilen der Zeile in Teile basierend auf dem Trennzeichen (Komma)
            String[] parts = value.toString().split(",");
            // Überprüfung, ob die Zeile keine Headerzeile ist
            if (!parts[0].equals("Customer")) {
                // Extrahiere relevante Felder
                String customerIdStr = parts[0];
                int pointsEarned = (int) Double.parseDouble(parts[7]); // Parse als double und dann in int casten
                int pointsRedeemed = (int) Double.parseDouble(parts[8]); // Parse als double und dann in int casten


                // Setze Werte in PointsWritable-Objekt
                valueOut.setPointsEarned(pointsEarned);
                valueOut.setPointsRedeemed(pointsRedeemed);

                // Emitiere Schlüssel-Wert-Paar (Kunden-ID, PointsWritable)
                customerId.set(customerIdStr);
                context.write(customerId, valueOut);
            }
        }
    }

    // Combiner-Klasse
    public static class PointsCombiner extends Reducer<Text, PointsWritable, Text, PointsWritable> {

        // Ergebnis-Objekt zum Speichern der kombinierten Punkte
        private final PointsWritable result = new PointsWritable();

        // Reduce-Funktion
        public void reduce(Text key, Iterable<PointsWritable> values, Context context)
                throws IOException, InterruptedException {
            // Initialisierung von Variablen zur Berechnung der Gesamtpunkte
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            // Iteriere über alle Werte für den aktuellen Schlüssel
            for (PointsWritable points : values) {
                // Summiere die verdienten und eingelösten Punkte
                totalPointsEarned += points.getPointsEarned();
                totalPointsRedeemed += points.getPointsRedeemed();
            }

            // Setze die kombinierten Punkte im Ergebnis-Objekt
            result.setPointsEarned(totalPointsEarned);
            result.setPointsRedeemed(totalPointsRedeemed);

            // Schreibe das Ergebnis (Schlüssel, kombinierte Punkte) in den Kontext
            context.write(key, result);
        }
    }

    public static class PointsReducer extends Reducer<Text, PointsWritable, Text, Text> {
        // Ergebnis-Objekt zum Speichern der Gesamtpunkte
        private final Text result = new Text();

        // Reduce-Funktion zur Verarbeitung der Werte für jeden Schlüssel (Kunden-ID)
        public void reduce(Text key, Iterable<PointsWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalPointsEarned = 0;
            int totalPointsRedeemed = 0;
            for (PointsWritable points : values) {
                totalPointsEarned += points.getPointsEarned();
                totalPointsRedeemed += points.getPointsRedeemed();
            }
            // Setze die Gesamtpunkte im Ergebnis-Objekt
            result.set(Integer.toString(totalPointsEarned) + " " + Integer.toString(totalPointsRedeemed));
            // Schreibe das Ergebnis (Kunden-ID und Gesamtpunkte) in den Kontext
            context.write(key, result);
        }
    }

    // Hauptmethode, die den gesamten MapReduce-Job konfiguriert und startet
    public static void main(String[] args) throws Exception {
        // Festlegen des Ausgabepfads für die Ergebnisse des Jobs
        final Path outPath = new Path("output");
        // Zugriff auf das Dateisystem für das Löschen des Ausgabeverzeichnisses (falls vorhanden)
        final FileContext fc = FileContext.getFileContext();

        // Löschen des Ausgabeverzeichnisses, um sicherzustellen, dass es leer ist
        fc.delete(outPath, true);

        // Konfiguration des MapReduce-Jobs
        Configuration conf = new Configuration();
        // Erstellen einer neuen Job-Instanz mit der angegebenen Konfiguration und einem Namen für den Job
        Job job = Job.getInstance(conf, "Fly Points Calculator");
        // Setzen der Klasse, die den ausführbaren Jar-Dateipfad enthält, für den MapReduce-Job
        job.setJarByClass(FlyPoint.class);
        // Setzen der Mapper-Klasse für den Job
        job.setMapperClass(PointsMapper.class);
        // Setzen des Datentyps für die Ausgabe des Mappers
        job.setMapOutputValueClass(PointsWritable.class);
        // Setzen der Combiner-Klasse für die lokale Aggregation der Mapper-Ausgabe
        job.setCombinerClass(PointsCombiner.class);
        // Setzen der Reducer-Klasse für den Job
        job.setReducerClass(PointsReducer.class);
        // Setzen des Datentyps für den Ausgabeschlüssel des Jobs
        job.setOutputKeyClass(Text.class);
        // Setzen des Datentyps für den Ausgabewert des Jobs
        job.setOutputValueClass(Text.class);

        // Hinzufügen des Eingabeverzeichnisses zum MapReduce-Job
        FileInputFormat.addInputPath(job, new Path("input"));
        // Festlegen des Ausgabeverzeichnisses für die Ergebnisse des Jobs
        FileOutputFormat.setOutputPath(job, outPath);

        // Starten des MapReduce-Jobs und Warten auf dessen Abschluss
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}