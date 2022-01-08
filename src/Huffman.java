
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Huffman {
    // Der Schlüssel zum setzen aller Buchstaben
    static Text transferKey = new Text("frequency");
    // Neue Mapper Klasse welche auf dem Hadoop Mapper basiert
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        // Hadoop Output Writable für Mapper -> Hier eine 1 damit später summiert werden kann
        private final static IntWritable writableOne = new IntWritable(1);

        // Hadoop Output Writeable Text -> Ändert sich mit jedem Iteriertem Buchstaben
        private Text character = new Text();
        // Map Funktion, die einen Context (File) bekommt, sowie den Input text des Songs
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Iterieren über den Input text und Ausgabe in Context
            String text = value.toString();
            StringTokenizer iterableText = new StringTokenizer(value.toString());

            for (int i = 0; i < value.toString().length(); i++){
                Character c = text.charAt(i);
                character.set(c.toString());
                context.write(character, writableOne);
            }
        }
    }

    // Neue Reducer Klasse, basierend auf Hadoop Reducer
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        // Hadoop output integer als writable -> Ergebnis Zählung der Wörter
        private IntWritable result = new IntWritable();

        // Reduce Funktion mit Input Werten (Values) zu gehörigem Key
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Summe der Anzahl der Buchstaben
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            // Ausgabe in Datei
            context.write(key, result);
        }
    }
    public static class TreeMapper extends Mapper<Object,Text,Text,Text>{

        @Override
//      Mapperfunktion für zusammenbringen aller Werte unter einem Schlüssel
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = value.toString().split("\t");
            String out = kv[0].toString() + ":" + kv[1].toString();
            Text outputValue = new Text(out);
            context.write(Huffman.transferKey, outputValue);
        }
    }
    public static class TreeReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            HuffmanTree tree = new HuffmanTree(values);
//      schreiben aller einträge der HashMap in die Ausgabedatei
            for (Map.Entry<String, String> entry:
            tree.mapTree().entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        }

    }
    // Hadoop Konfiguration & Initierung
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Configuration conf2 = new Configuration();
        Job job1 = Job.getInstance(conf, "lettercount");
        Job job2 = Job.getInstance(conf2, "huffman");
//      Erster Job mit Combiner
        job1.setJarByClass(Huffman.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
//      Zweiter Job ohne Combiner
        job2.setJarByClass(Huffman.class);
        job2.setMapperClass(TreeMapper.class);
        job2.setReducerClass(TreeReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
