/*
* ---------------------- CloudFlix: Movie feature-set generation code ------------------------
*        Author: Saurav Majumder
*        Date: Nov 14, 2013
*        Description: This is the main class to create the final set of features for the 
*        movie classification.
*        @param Input files: movie.dat, rating.dat and genre.dat.
* --------------------------------------------------------------------
*/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class createFeatureVectorFiles {
	/* HashMap to keep feature vector of every movie. <movie-id,feature-set> */
	HashMap<String, movieFeature>movieMap;
	newDataset nd;
	sentimentScore sScore;
	Set<String> gen;
	final String bucketName = "cloudflixbucket";
	AmazonS3 s3;
	Date date;
	
	public createFeatureVectorFiles() {
		date= new Date();
		s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
		System.out.println("[" + new Timestamp(date.getTime()) + "] Amazon S3 connection established.");
	}
	
	/* Function read the feature-set for each movie and stores it into the map mentioned above */
	public void readFeatures(String featureFile, String genreFile, String sentimentFile) throws IOException{
		BufferedReader br;
		String         line;
		movieFeature   movie; /* Variable of type movieFeature class which is in this package */
		String[] 	   features;
		
		movieMap = new HashMap<>();
		
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, featureFile));
		System.out.println("[" + new Timestamp(new Date().getTime()) + "] Downloading movie feature file completed.");
        
        br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		
		/* Read each line in the file and extracts the required features and puts it into the map */
		while ((line = br.readLine()) != null) {
			features = line.split("\t"); /* Split the input file with tab delimiter, <Change if required> */
			/* Create object of the movie class containing all the features of the movie */
		    movie = new movieFeature(features[0], features[1], features[2], features[5], features[6], features[7], features[8], features[9],
		    		 features[10], features[11], features[12], features[13], features[14], features[15], features[16], features[17], features[18],
		    		 features[19], features[21], features[22]);
		    movieMap.put(movie.mId, movie); /* Put into the map */
		}
		
		System.out.println("[" + new Timestamp(date.getTime()) + "] Reading movei feature file completed.");
		/*Dereference all pointers */
		br.close();
		br = null;
		
		/* Extract genre for all the movies and store in the LinkedHashSet */
		nd = new newDataset(genreFile);
		System.out.println("[" + new Timestamp(new Date().getTime()) + "] Movie genre data-structure prepared.");
		
		sScore = new sentimentScore(sentimentFile);
		System.out.println("[" + new Timestamp(new Date().getTime()) + "] Dowloading user semtiment analysis score completed.");
		
		/* Retrieve list of all types of genre included in that file */
		gen = nd.allGenre();
	}
	
	/* Function combines the feature values stored in the previous function to create final feature vector for each user */
	public void createTrainingSet(String ratingFile, String opFolder) throws IOException {
		BufferedReader 	br;
		String         	line;
		Iterator<String>iterator;
		String 			vector = null;
		File			outputFile = null;
		
		int i = 0;
		String currentUserId = "-1"; /* Set current user to -1 for starting */
		String[] values;
		String mvclass = "negative";
		Writer writer = null;
		movieFeature mf;
		
		/* Check if the movie features have been extracted or not */
		if(movieMap.size() < 1) return;
		
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, ratingFile));
		System.out.println("[" + new Timestamp(date.getTime()) + "] Reading user rating file completed.");     
		br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

		/* Retrive the feature names from the movie map to include in the beginning of the final feature set */
		mf = movieMap.get("id"); /* Search with 'id' since it his the value of the id of that row */
		String attribute = "id, movie_id, " + mf.rtAllCriticsRating + ", " + mf.rtAllCriticsNumReviews + ", " + mf.rtAllCriticsNumFresh + ", " + mf.rtAllCriticsNumRotten
					 + ", " + mf.rtAllCriticsScore + ", " + mf.rtTopCriticsRating + ", " + mf.rtTopCriticsNumReviews + ", " + mf.rtTopCriticsNumFresh
					 + ", " + mf.rtTopCriticsNumRotten + ", " + mf.rtTopCriticsScore + ", " + mf.rtAudienceRating + ", " + mf.rtAudienceNumRatings + ", " + mf.rtAudienceScore
					 + ", " + mf.Directors + ", ";
		
		/* Iterate through the set to print the name of the genres in the first row of the final list. */
		iterator = gen.iterator(); 
		while (iterator.hasNext()){
			attribute += iterator.next() + ", ";  
		}
		
		attribute += "SentimentAnalysisScore ,";
		
		/* Add the feature name 'Class' in the end of the first row */
		attribute += "Class";
		
		//int stopCounter = 50;
		/* Read each line in the user rating file to retrieve user's liked and disliked movies */
		while ((line = br.readLine()) != null) {
			values = line.split(",");
			
			/* If the current user is not same as the previous then start a new file */
			if(!currentUserId.equals(values[0])) { 
				if(!currentUserId.equals("-1")) {
					writer.close();
					s3.putObject(new PutObjectRequest(bucketName, opFolder + "/cloudflixuser_"+currentUserId+".csv", outputFile));
					System.out.println("[" + new Timestamp(date.getTime()) + "] Uploading training set for user#"+currentUserId+" completed.");
					//stopCounter--;
					//if(stopCounter < 1) break;
				}
				currentUserId = values[0]; /* Create new file with the user id */
				
				outputFile = File.createTempFile("cloudflixuser_"+currentUserId, ".csv");
				outputFile.deleteOnExit();
				writer = new OutputStreamWriter(new FileOutputStream(outputFile));
				writer.write(attribute);
				writer.write("\n");
				i = 1;
			}
			
			/* Adding Movie feature vector to the final list */
			if(movieMap.containsKey(values[1])){
				mf = movieMap.get(values[1]); /* Retrieving feature vector of the specified movie */
				/* Creating the feature vector */
				vector = i++ + ", " + mf.mId + ", " + mf.rtAllCriticsRating + ", " + mf.rtAllCriticsNumReviews + ", " + mf.rtAllCriticsNumFresh + ", " + mf.rtAllCriticsNumRotten
						 + ", " + mf.rtAllCriticsScore + ", " + mf.rtTopCriticsRating + ", " + mf.rtTopCriticsNumReviews + ", " + mf.rtTopCriticsNumFresh
						 + ", " + mf.rtTopCriticsNumRotten + ", " + mf.rtTopCriticsScore + ", " + mf.rtAudienceRating + ", " + mf.rtAudienceNumRatings + ", " + mf.rtAudienceScore + ", " + mf.Directors;
			}
			
			/* Adding Movie genre in bag of word format in the final list */
			iterator = gen.iterator(); 
			/* Check for genres which belong to the specified movie */
			while (iterator.hasNext()){
				int checkGenre = nd.isGenre(values[1], iterator.next()) ? 1 : 0; /* Set 1 if the movie belong to that genre or set 0 */
				vector += ", "+ checkGenre;  
			}
			
			vector += ", "+ sScore.posPercentage(values[1]); 
			
			/* Adding movie class in the final list */
			float rating = Float.parseFloat(values[2]);
			
			/* If rating is greater than 3 then consider as positive, or else negative */
			if(rating > 3) {
				mvclass = "positive";
			} else {
				mvclass = "negative";
			}
			vector += ", " + mvclass;
			
			writer.write(vector);
			writer.write("\n");
			writer.flush();
		}
	
		writer.close();
		s3.putObject(new PutObjectRequest(bucketName, opFolder + "/cloudflixuser_"+currentUserId+".csv", outputFile));
		
		System.out.println("[" + new Timestamp(date.getTime()) + "] Uploading training set for user#"+currentUserId+" completed.");

		br.close();
		System.out.println("[" + new Timestamp(date.getTime()) + "] Training set compilation completed...");
		return;
	}
	
	public void createTestSet(String testFolder, String opFolder){
		S3Object 		object;
		movieFeature	mf;
		BufferedReader 	br = null;
		Writer 			writer = null;
		Iterator<String>iterator;
		String 			line;
		String 			vector = null;
		File			outputFile = null;
		
		System.out.println("[" + new Timestamp(date.getTime()) + "] Starting test case creation.");
		mf = movieMap.get("id");
		String attribute = "id, movie_id, " + 
							mf.rtAllCriticsRating + ", " + 
							mf.rtAllCriticsNumReviews + ", " + 
							mf.rtAllCriticsNumFresh + ", " + 
							mf.rtAllCriticsNumRotten + ", " + 
							mf.rtAllCriticsScore + ", " + 
							mf.rtTopCriticsRating + ", " + 
							mf.rtTopCriticsNumReviews + ", " + 
							mf.rtTopCriticsNumFresh + ", " + 
							mf.rtTopCriticsNumRotten + ", " + 
							mf.rtTopCriticsScore + ", " + 
							mf.rtAudienceRating + ", " +
							mf.rtAudienceNumRatings + ", " + 
							mf.rtAudienceScore + ", " + 
							mf.Directors + ", ";
		  
		iterator = gen.iterator(); 
		while (iterator.hasNext()){
			attribute += iterator.next() + ", ";  
		}				
		attribute += "SentimentAnalysisScore ,";
		attribute += "Class";
		
		//int stopCounter = 50;
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(testFolder+"/collabReco_"));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			//stopCounter--;
			//if(stopCounter < 1) break;
			try {
				int rec_id = 1;
				
				String movie_id = (((objectSummary.getKey().toString()).split("\\/")[1]).split("\\.")[0]).split("_")[1];
				
				object = s3.getObject(new GetObjectRequest(bucketName, objectSummary.getKey()));
				br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
				
				String opFilename = "testMovie_" + movie_id;
				outputFile = File.createTempFile(opFilename, ".csv");
				outputFile.deleteOnExit();
				writer = new OutputStreamWriter(new FileOutputStream(outputFile));
				
				writer.write(attribute);
				writer.write("\n");
				
				while ((line = br.readLine()) != null) {
					String mvid = line.trim();
					
					if(line.length() < 1) continue;
					
					if(movieMap.containsKey(mvid)){
						mf = movieMap.get(mvid);
						
						vector = rec_id++ + ", " + mvid + ", " + 
								mf.rtAllCriticsRating + ", " + 
								mf.rtAllCriticsNumReviews + ", " + 
								mf.rtAllCriticsNumFresh + ", " + 
								mf.rtAllCriticsNumRotten + ", " + 
								mf.rtAllCriticsScore + ", " + 
								mf.rtTopCriticsRating + ", " + 
								mf.rtTopCriticsNumReviews + ", " + 
								mf.rtTopCriticsNumFresh + ", " + 
								mf.rtTopCriticsNumRotten + ", " + 
								mf.rtTopCriticsScore + ", " + 
								mf.rtAudienceRating + ", " + 
								mf.rtAudienceNumRatings + ", " + 
								mf.rtAudienceScore + ", " + 
								mf.Directors;
					}
					
					/* Adding Movie genre in bag of word format in the final list */
					iterator = gen.iterator(); 
					
					/* Check for genres which belong to the specified movie */
					while (iterator.hasNext()){
						int checkGenre = nd.isGenre(mvid, iterator.next()) ? 1 : 0; /* Set 1 if the movie belong to that genre or set 0 */
						vector += ", "+ checkGenre;  
					}
					
					vector += ", "+ sScore.posPercentage(mvid); 
					vector += ", positive";
					
					writer.write(vector);
					writer.write("\n");
				}
				
				writer.close();
				s3.putObject(new PutObjectRequest(bucketName, opFolder + "/" + opFilename + ".csv", outputFile));
				System.out.println("[" + new Timestamp(date.getTime()) + "] Uploading test case "+ opFilename +".csv completed.");
				
				br.close();				
		        
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
		
		System.out.println("[" + new Timestamp(date.getTime()) + "] Test set created successfuly.");
	}
	
	public void preStdart() {
		
	}
	
	public static void main(String[] args) {
		createFeatureVectorFiles user = new createFeatureVectorFiles();
		try {
			user.readFeatures("movie.dat", "moviesraw.dat", "sentimentResult.txt");
		
			user.createTrainingSet("ratingsForMahout.csv", "TrainingSet");
			user.createTestSet("collaborative_result", "TestSet");
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

}

