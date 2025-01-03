import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UnigramIndex {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      Text docID = new Text(itr.nextToken().strip());
      // System.out.println("DocID = "+ docID.toString());
      String nextWord = "";
      while (itr.hasMoreTokens()) {
        nextWord = itr.nextToken().replaceAll("[^A-Za-z]+", " ").toLowerCase();
        StringTokenizer wordTok = new StringTokenizer(nextWord);
        String wordString = "";
        while (wordTok.hasMoreTokens()) {
          wordString = wordTok.nextToken().strip();
          if (wordString.isBlank()) {
            continue;
          }
          word.set(wordString);
          context.write(word, docID);
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashMap<String, Integer> doc_freq = new HashMap<>();
      for (Text val : values) {
        int sum = 1;
        String docID = val.toString();
        // System.out.println("value = "+ docID);
        if (doc_freq.containsKey(docID)) {
          sum = doc_freq.get(docID) + 1;
        }
        doc_freq.put(docID, sum);
      }
      String resultStr = "";
      for (String docID : doc_freq.keySet()) {
        if (docID.contains(":")) {
          resultStr += docID + " ";
        } else {
          resultStr += docID + ":" + doc_freq.get(docID) + " ";
        }
      }
      result.set(resultStr);
      // System.out.println("reduce called = "+ key.toString() + " " + doc_freq.size()
      // + " " + doc_freq.keySet().size() + " " + resultStr);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Unigram Index");

    job.setJarByClass(UnigramIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}// UnigramIndex
