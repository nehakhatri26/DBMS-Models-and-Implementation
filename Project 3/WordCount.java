import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {

  public static class TokenizerMapper extends Mapper<Object , Text, Text, IntWritable>{

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {


     String[] token = line.toString().split(";");                                                  //Split the line on - ;

     String get_year = token[3];                                                                   //Get the third index which is year
     String get_genre=token[4].replaceAll("\n", "");                                              // Get the fourth index which is genre 
     String [] new_genre=get_genre.split(",");
                
     for(int i=0;i<new_genre.length;i++){
		
       if(get_year.length()==4 && get_year.matches("^\\d+$") && !new_genre[i].equals("\\N")){       //Check if the record is valid

     	 if(Integer.valueOf(get_year)>=2000){                                                      // Get the year > 2000
                          
            String result = get_year+","+new_genre[i];                                            // Store year,genre in the result
 	    context.write(new Text(result), new IntWritable(1));
   
	  }
		 
        }
      }
    }
 }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();                //count the number of values for the same key
      }
      result.set(sum);
       context.write(key, result);      //write the result
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
