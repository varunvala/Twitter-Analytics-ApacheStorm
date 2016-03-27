package student.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

class TopNTweetTopology
{
  public static void main(String[] args) throws Exception
  {
    int TOP_N = 5;
    TopologyBuilder builder = new TopologyBuilder();
    TweetSpout tweetSpout = new TweetSpout(
            "",
            "",
            "",
            ""
    );

    builder.setSpout("tweet-spout", tweetSpout, 1);

    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");
    builder.setBolt("infoBolt", new InfoBolt(), 10).fieldsGrouping("parse-tweet-bolt", new Fields("county_id"));
    builder.setBolt("top-words", new TopWords(), 10).fieldsGrouping("infoBolt", new Fields("county_id"));
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("top-words");

    Config conf = new Config();

    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {
      conf.setMaxTaskParallelism(4);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());
      Utils.sleep(300000000);
      cluster.killTopology("tweet-word-count");
      cluster.shutdown();
    }
  }
}
