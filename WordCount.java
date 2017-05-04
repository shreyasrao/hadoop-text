	import java.io.IOException;
	import java.util.*;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
	import org.apache.hadoop.util.*;
	import org.apache.commons.collections.*;
	
	public class WordCount {
	
	   
	   	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     
            private Text word = new Text();
            private Text query = new Text();
		
		     
		    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
                
                //convert every character into lower-case, any character other than a-z, A-Z,
                //    or 0-9 should be replaced by a single-space character.

		     	String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\d\\s]", " ");
		       	StringTokenizer contextTokens = new StringTokenizer(line);
				Set<String> seenBefore = new HashSet<String>();
                
                //To observe how many times words occur with each other in a given text,
                //    split sentence into conxext words and query words                
                
                //Output of Mapper is <contextWord, queryWord> key value pairs
                
		       	while (contextTokens.hasMoreTokens()) {
		       		String contextWord = contextTokens.nextToken();
					if(seenBefore.contains(contextWord)){}
					else{
						seenBefore.add(contextWord);
                        word.set(contextWord);
			       		StringTokenizer queryTokens = new StringTokenizer(line);
			       		Boolean firstFind = false;
			       		while (queryTokens.hasMoreTokens()){
			       			String queryWord = queryTokens.nextToken();
			       			if(contextWord.equals(queryWord)){
								if(firstFind){
				       				query.set(queryWord);
									output.collect(word, query);
								}
			       				else{firstFind = true;}
			       			}
			       			else{
			       				query.set(queryWord);
								output.collect(word, query);
			       			}

			       		}
					}
		       	}
		    }
	   	}
	

	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	   	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            //Output of Reduce needs to be the form:
            //contextword1
            //<queryword1, occurrence>
            //<queryword2, occurrence>
            //            
            //contextword2
            //<queryword1, occurrence>
            //<queryword2, occurrence>
	   		
            String o = "";
			String contextWord = key.toString() + "\n";
			o += contextWord;	   		
			Bag queryWords = new HashBag();
	   		
            while(values.hasNext()){
				Text t = values.next();
				String currVal = t.toString();
	   			queryWords.add(currVal,1);
	   		}

	   		Set<String> uniqueQueries = queryWords.uniqueSet();
	   		for(String query : uniqueQueries){
	   			o+= "<"+ query + ", "  + Integer.toString(queryWords.getCount(query)) + ">\n"; 
	   		}
	   		Text outputText = new Text(o);
			
            //In order to keep output format exact, we must store entire output as the key. Value is blank
            output.collect(outputText, new Text(""));
	     }
	   }
	
	   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(WordCount.class);
	     conf.setJobName("wordcount");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     //conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	   }
	}
