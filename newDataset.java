/*
* ---------------------- CloudFlix: Class for handling movie genre ------------------------
*        Author: Saurav Majumder
*        Date: Nov 14, 2013
*        Description: This is the class containing all the genre information related to 
*        the movies.
*        @param Input files: movie genre file.
* -----------------------------------------------------------------------------------
*/

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class newDataset {
	protected HashMap<String,movieGenre>movie_map;
	Set<String>		allGenre;
	final String bucketName = "cloudflixbucket";
	AmazonS3 s3;
	
	public newDataset(String genrefile) {
		BufferedReader 	br;
		String         	line;
		String[]		features;
		String[]		genre;
		movieGenre		mg;
		
		s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
		movie_map = new HashMap<String,movieGenre>(); /* Map to contain all the movies and its genres */
		allGenre = new HashSet<String>(); /* Set to contain all the genres */
		
		try {
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, genrefile));
	        //System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
	        br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
	        
			//fis = new FileInputStream(genrefile);
			//br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
			
			while ((line = br.readLine()) != null) {
				//System.out.println("Line: "+line);
				features = line.split("::");
				if(features.length > 1) {
					mg = new movieGenre(features[0],features[1]);
					genre = features[2].split("\\|");
					for(String s : genre) {
						mg.addGenre(s);
						allGenre.add(s);
				    }
					//System.out.println(mg.movie_id+"::"+mg.movie_name);
					movie_map.put(features[0], mg);
				}
			}
			br.close();
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	/* Check if a specified movie belongs to a certain genre */
	public boolean isGenre(String movie_id, String Genre) {
		if(movie_map.containsKey(movie_id)) {
			Set<String> genreList = movie_map.get(movie_id).getAllGenre();
			if(genreList.contains(Genre)) {
				return true;
			} else {
				return false;
			}
		}
		return false;
	}
	
	/* Return set of all genres present in the input file */
	public Set<String> allGenre() {
		return allGenre;
	}
}
