package udacity.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Sridhar Somasani
 *
 * This bolt parses all incoming tweets with country id and emits only hashtags and country id
 */
public class ParseTweetBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;
  StringBuilder result;
  
//  private String[] skipWords = {"rt", "to", "me","la","on","that","que",
//    "followers","watch","know","not","have","like","I'm","new","good","do",
//    "more","es","te","followers","Followers","las","you","and","de","my","is",
//    "en","una","in","for","this","go","en","all","no","don't","up","are",
//    "http","http:","https","https:","http://","https://","with","just","your",
//    "para","want","your","you're","really","video","it's","when","they","their","much",
//    "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
//    "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were","TWITTER",
//    "make","take","This","from","about","como","esta","follows","followed"};

  
  
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;

  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the 1st column 'tweet' from tuple
    String tweet = tuple.getStringByField("tweet");
    String country_id = tuple.getStringByField("country-id");
    
    //tweet = tweet.replaceAll("[^a-zA-Z0-9# .,?!]+", "");
    
    // provide the delimiters for splitting the tweet
    String delims = "[ .,?!]+";
    
	String[] tokens = tweet.split(delims);
  	int n = tokens.length;
    for (int i = 0; i < n; i++) {
    	String token = tokens[i];
    	if(token.startsWith("#") && token.length() > 2){
    		collector.emit(new Values(token.toLowerCase(), country_id));
   	      }
    }

    

    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("hashtag", "country-id"));
  }

}
