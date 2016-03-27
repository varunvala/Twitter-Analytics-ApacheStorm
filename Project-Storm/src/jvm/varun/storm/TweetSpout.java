package varun.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import student.storm.tools.SentimentAnalyzer;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
import twitter4j.URLEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TweetSpout extends BaseRichSpout
{
  String custkey, custsecret;
  String accesstoken, accesssecret;
  SpoutOutputCollector collector;

  TwitterStream twitterStream;
  LinkedBlockingQueue<String> queue = null;
  
  Pattern moodPattern = Pattern.compile("love|hate|happy|angry|sad");
  Pattern properPattern = Pattern.compile("^[a-zA-Z0-9 ]+$");
  private class TweetListener implements StatusListener {

    @Override
    public void onStatus(Status status)
    {
    	String geoInfo = "37.7833,122.4167";
    	String urlInfo = "n/a";
    	if(status.getGeoLocation() != null)
    	{
    		geoInfo = String.valueOf(status.getGeoLocation().getLatitude()) + "," + String.valueOf(status.getGeoLocation().getLongitude());
        	if(status.getURLEntities().length > 0)
        	{
        		for(URLEntity urlE: status.getURLEntities())
        		{
        			urlInfo = urlE.getURL();
        		}         
        	}
       	   queue.offer(status.getText() + "DELIMITER" + geoInfo + "DELIMITER" + urlInfo);
    	}
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn)
    {
    }

    @Override
    public void onTrackLimitationNotice(int i)
    {
    }

    @Override
    public void onScrubGeo(long l, long l1)
    {
    }

    @Override
    public void onStallWarning(StallWarning warning)
    {
    }

    @Override
    public void onException(Exception e)
    {
      e.printStackTrace();
    }
  };

  /**
   * Constructor for tweet spout that accepts the credentials
   */
  public TweetSpout(
      String                key,
      String                secret,
      String                token,
      String                tokensecret)
  {
    custkey = key;
    custsecret = secret;
    accesstoken = token;
    accesssecret = tokensecret;
  }

  @Override
  public void open(
      Map                     map,
      TopologyContext         topologyContext,
      SpoutOutputCollector    spoutOutputCollector)
  {
    queue = new LinkedBlockingQueue<String>(1000);
    SentimentAnalyzer.init();
    collector = spoutOutputCollector;


    ConfigurationBuilder config =
        new ConfigurationBuilder()
               .setOAuthConsumerKey(custkey)
               .setOAuthConsumerSecret(custsecret)
               .setOAuthAccessToken(accesstoken)
               .setOAuthAccessTokenSecret(accesssecret);

    TwitterStreamFactory fact =
        new TwitterStreamFactory(config.build());

    twitterStream = fact.getInstance();    
    
    FilterQuery tweetFilterQuery = new FilterQuery(); // See 
    tweetFilterQuery.locations(new double[][]{new double[]{-124.848974,24.396308},
                    new double[]{-66.885444,49.384358
                    }}); 
    tweetFilterQuery.language(new String[]{"en"});

    
 
    
    twitterStream.addListener(new TweetListener());

    twitterStream.filter(tweetFilterQuery);

    twitterStream.sample();
  
  }

  @Override
  public void nextTuple()
  {
    String ret = queue.poll();
    String geoInfo;
    String originalTweet;
    if (ret==null)
    {
      Utils.sleep(50);
      return;
    }
    else
    {
        geoInfo = ret.split("DELIMITER")[1];
        originalTweet = ret.split("DELIMITER")[0];
    }
    
    if(geoInfo != null && !geoInfo.equals("n/a"))
    {
        System.out.print("\t DEBUG SPOUT: BEFORE SENTIMENT \n");
        int sentiment = SentimentAnalyzer.findSentiment(originalTweet)-2;
        System.out.print("\t DEBUG SPOUT: AFTER SENTIMENT (" + String.valueOf(sentiment) + ") for \t" + originalTweet + "\n");
        collector.emit(new Values(ret, sentiment));
    }
  }

  @Override
  public void close()
  {
    twitterStream.shutdown();
  }

  /**
   * Component specific configuration
   */
  @Override
  public Map<String, Object> getComponentConfiguration()
  {
    Config ret = new Config();

    ret.setMaxTaskParallelism(1);

    return ret;
  }

  @Override
  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("tweet", "sentiment"));
  }
}
