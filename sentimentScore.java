import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Pattern;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class sentimentScore {
	HashMap<String, int[]>sentiment; 
	
	public sentimentScore(String inputPath) {
		BufferedReader 	br;
		String         	line,rest;
		String[]		items;
		String[]		scores;
		int[]			score;
		String			movie_id;
		final String 	bucketName = "cloudflixbucket";
		AmazonS3 		s3;
		
		sentiment = new HashMap<String, int[]>();
		
		try {
			s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());		
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, inputPath));
	        //System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
	        br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
	        
			while ((line = br.readLine()) != null) {
				//System.out.println(line);

				items = line.split("\t");
				movie_id = items[0].replaceAll("^\"|\"$", "");
				score = new int[3];
				rest = items[1].replaceAll("^\"|\"$", "");
				scores = rest.split(Pattern.quote("||"));
				
				score[0] = Integer.parseInt(scores[0]);
				score[1] = Integer.parseInt(scores[1]);
				score[2] = Integer.parseInt(scores[2]);
				
				//System.out.println("ID: "+movie_id+" Rest: "+ score[0]);

				sentiment.put(movie_id, score);
			}
			br.close();
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public float posPercentage(String movie_id) {
		if(sentiment.containsKey(movie_id)) {
			int[] scores = sentiment.get(movie_id);
			float res = (float)scores[0]/(float)scores[2];
			//System.out.println(scores[0]+"/"+scores[2]+"="+res);
			return (res);
		}
		return -1;
	}
}
