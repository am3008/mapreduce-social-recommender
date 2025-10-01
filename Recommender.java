import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Recommender {

    public static class MutualFriendsMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text uKey = new Text();
        private Text fVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] tokens = value.toString().split("\t");
            if (tokens.length < 2) return;

            String user = tokens[0];
            String[] friends = tokens[1].split(",");

            for (int i = 0; i < friends.length; i++) 
            {
                String friendA = friends[i];
                uKey.set(user);
                fVal.set("FRIEND," + friendA);
                context.write(uKey, fVal);

                for (int j = i + 1; j < friends.length; j++) 
                {
                    String friendB = friends[j];
                    if (!friendA.equals(friendB)) 
                    {
                        uKey.set(createSortedPair(friendA, friendB));
                        fVal.set("MUTUAL," + user);
                        context.write(uKey, fVal);
                    }
                }
            }
        }

        private String createSortedPair(String a, String b) 
        {
            int userA = Integer.parseInt(a.trim());
            int userB = Integer.parseInt(b.trim());
            return (userA < userB) ? userA + "," + userB : userB + "," + userA;
        }
    }

    public static class MutualFriendsReducer extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            String[] users = key.toString().split(",");
            Map<String, Integer> mutuals = new HashMap<>();
            boolean directFriend = false;

            for (Text val : values) 
            {
                String value = val.toString();
                String[] tokens = value.split(",", 2);
                if (tokens.length < 2) continue;
                String relationType = tokens[0];
                String relatedUser = tokens[1];

                if (relationType.equals("FRIEND")) 
                {
                    directFriend = true;
                    break; 
                } 
                else if (relationType.equals("MUTUAL")) 
                {
                    mutuals.put(relatedUser, mutuals.getOrDefault(relatedUser, 0) + 1);
                }
            }

            if (!directFriend && mutuals.size() > 0) 
            {
                String userA = users[0];
                String userB = users[1];
                int mutualCount = mutuals.size();

                context.write(new Text(userA), new Text(userB + "," + mutualCount));
                context.write(new Text(userB), new Text(userA + "," + mutualCount));
            }
        }
    }

    public static class RecommendationAggregationMapper extends Mapper<Object, Text, Text, Text> 
    {
        private Text uKey = new Text();
        private Text recVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] tokens = value.toString().split("\t");
            if (tokens.length < 2) return;

            String user = tokens[0];
            String recommendations = tokens[1];

            uKey.set(user);
            recVal.set(recommendations);
            context.write(uKey, recVal);
        }
    }

    public static class RecommendationAggregationReducer extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            Map<String, Integer> recs = new HashMap<>();

            for (Text val : values) 
            {
                String[] tokens = val.toString().split(",");
                if (tokens.length == 2) 
                {
                    String recommendedUser = tokens[0].trim();
                    int mutualCount = Integer.parseInt(tokens[1].trim());
                    recs.put(recommendedUser, mutualCount);
                }
            }

            List<Map.Entry<String, Integer>> sortedRecs = new ArrayList<>(recs.entrySet());
            sortedRecs.sort((a, b) -> {
                int countCompare = b.getValue().compareTo(a.getValue()); 
                if (countCompare != 0)
                {
                    return countCompare;
                } 
                else 
                {
                    return a.getKey().compareTo(b.getKey()); 
                }
            });

            StringBuilder topRecs = new StringBuilder();
            int maxRecs = Math.min(10, sortedRecs.size());
            for (int i = 0; i < maxRecs; i++) {
                topRecs.append(sortedRecs.get(i).getKey());
                if (i < maxRecs - 1) 
                {
                    topRecs.append(",");
                }
            }

            context.write(key, new Text(topRecs.toString()));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Mutual Friends Recommendation - Job 1");
        job1.setJarByClass(Recommender.class);
        job1.setMapperClass(MutualFriendsMapper.class);
        job1.setReducerClass(MutualFriendsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediateOutput = new Path("intermediate_output_new");
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (job1.waitForCompletion(true)) 
        {
            Job job2 = Job.getInstance(conf, "Mutual Friends Recommendation - Job 2");
            job2.setJarByClass(Recommender.class);
            job2.setMapperClass(RecommendationAggregationMapper.class);
            job2.setReducerClass(RecommendationAggregationReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, intermediateOutput);
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
