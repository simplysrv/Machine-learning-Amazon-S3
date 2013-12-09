/*
* ---------------------- CloudFlix: Movie classification code ------------------------
*        Author: Saurav Majumder
*        Date: Nov 19, 2013
*        Description: This file classifies movies to generate set of recommended movies for a user.
*        @param <training set> <test set> <user id>
* ------------------------------------------------------------------------------------
*/
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;
import java.sql.Timestamp;
import java.io.File;

/* ------------ Machine Learning Algorithms ----------- */
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;

/* ------------ Machine Learning Supporting libs ---------- */
import weka.core.Instances;
import weka.classifiers.Evaluation;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;
import weka.core.converters.CSVLoader;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class movieRecommend {
	final String bucketName;
	final String trainFolder;
	final String testFolder;
	final String resultFolder;
	final String statFolder;
	AmazonS3 s3;
	
	public movieRecommend(String bucket, String trainFolder, String testFolder, String result, String stat){
		this.bucketName = bucket;
		this.trainFolder = trainFolder;
		this.testFolder = testFolder;
		this.resultFolder = result;
		this.statFolder = stat;
			
		s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
		System.out.println("Amazon S3 connection established....");
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(trainFolder+"/cloudflixuser_"));
		 
		//int stopCounter = 50;
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			try {
				//stopCounter--;
				//if(stopCounter < 1) break;
				
				String user_id = (((objectSummary.getKey().toString()).split("\\/")[1]).split("\\.")[0]).split("_")[1];
				//System.out.println(trainFolder+"/cloudflixuser_"+user_id + " || " + testFolder + "/testMovie_"+user_id +" || "+user_id);
				testNB(trainFolder+"/cloudflixuser_"+user_id + ".csv", testFolder + "/testMovie_" + user_id + ".csv", user_id);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		new movieRecommend("cloudflixbucket", "TrainingSet", "TestSet", "predictionresult", "predictionstatistics");
	}
	
	public void testNB(String trainingSet, String testSet, String userId) throws Exception{
		if(trainingSet == "" || testSet == "" || userId == "")
			return;
		java.util.Date date= new java.util.Date();
		
		//System.out.println("[Final Classification]["+new Timestamp(date.getTime())+"] Training set: " + trainingSet + " | Test set: " + testSet + " | User ID: " + userId);
		
		CSVLoader loader = new CSVLoader();

		double[] pred_val;
		int pos = 0,neg = 0;
		String final_prediction="";
		Writer writer = null;
		File outputFile = null;
		
		NaiveBayes nb = new NaiveBayes();
		J48 dt = new J48();
		RandomForest rf = new RandomForest();
		
		// Training Classifier.
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, trainingSet));      
		File trainSet = createFile(object.getObjectContent());	
		loader.setSource(trainSet);	    
		Instances train = loader.getDataSet();
		train.setClassIndex(train.numAttributes() - 1);
		
		// Remove user-id from feature-set.
		String[] options = new String[2];
		options[0] = "-R";
		options[1] = "1";
		Remove remove = new Remove();
		remove.setOptions(options);
		remove.setInputFormat(train);
		Instances newTrain = Filter.useFilter(train, remove);
		newTrain.setClassIndex(newTrain.numAttributes() - 1);
		System.out.println("["+new Timestamp(date.getTime())+"] Reading training set completed.");
		
		nb.buildClassifier(newTrain);
		System.out.println("["+new Timestamp(date.getTime())+"] Training Naive Bayes completed.");
		dt.buildClassifier(newTrain);
		System.out.println("["+new Timestamp(date.getTime())+"] Training Decision Tree completed.");
		rf.buildClassifier(newTrain);
		System.out.println("["+new Timestamp(date.getTime())+"] Training RandomForest completed.");

		// Testing unlabeled data.
		object = s3.getObject(new GetObjectRequest(bucketName, testSet));    
		File testingSet = createFile(object.getObjectContent());	
		loader.setSource(testingSet);	 
		Instances test = loader.getDataSet();
		test.setClassIndex(test.numAttributes() - 1);
		
		Instances newTest = Filter.useFilter(test, remove);
		newTest.setClassIndex(newTest.numAttributes() - 1);
		
		//Instances newTest = Filter.useFilter(test, remove);
		System.out.println("["+new Timestamp(date.getTime())+"] Reading test set completed.");
		
		outputFile = File.createTempFile("recommend_"+userId, ".dat");
		outputFile.deleteOnExit();
		writer = new OutputStreamWriter(new FileOutputStream(outputFile));
		
		for (int i = 0; i < newTest.numInstances(); i++) {
			pos = 0;
			neg = 0;
			pred_val = new double[3];
			pred_val[0] = nb.classifyInstance(newTest.instance(i));
			pred_val[1] = dt.classifyInstance(newTest.instance(i));
			pred_val[2] = rf.classifyInstance(newTest.instance(i));
			
			for(int j = 0; j < 3; j++) {
				if((int) pred_val[j] == 0) {
					pos++;
				} else if((int) pred_val[j] == 1) {
					neg++;
				} else {
					neg--;
				}
			}
			
			final_prediction = pos > neg ? "positive" : "negative";
			
			if(final_prediction.equals("positive")) {
				writer.write(Integer.toString((int)newTest.instance(i).value(0)));
				writer.write("\n");
			}
		}
		
		writer.flush();
		writer.close();
		s3.putObject(new PutObjectRequest(bucketName, resultFolder + "/recommend_"+userId+".dat", outputFile));		
		System.out.println("["+new Timestamp(date.getTime())+"] Storing classification result completed.");
		
		outputFile = File.createTempFile("PredictionSummary_"+userId, ".dat");
		outputFile.deleteOnExit();
		writer = new OutputStreamWriter(new FileOutputStream(outputFile));
		
		Evaluation eval = new Evaluation(newTrain);
		
		System.out.println("["+new Timestamp(date.getTime())+"] Cross-validating Naive Bayes.");
		eval.crossValidateModel(nb, newTrain, 10, new Random(1));
		writer.write(eval.toSummaryString("\n Naive Bayes Result\n============================\n", true));
		
		System.out.println("["+new Timestamp(date.getTime())+"] Cross-validating Decision Tree.");
		eval.crossValidateModel(dt, newTrain, 10, new Random(1));
		writer.write(eval.toSummaryString("\n Decision Tree Result\n============================\n", true));
		
		System.out.println("["+new Timestamp(date.getTime())+"] Cross-validating Random Forest.");
		eval.crossValidateModel(rf, newTrain, 10, new Random(1));
		writer.write(eval.toSummaryString("\n Random Forest Result\n============================\n", true));
		
		writer.flush();
		writer.close();
		s3.putObject(new PutObjectRequest(bucketName, statFolder + "/PredictionSummary__"+userId+".dat", outputFile));
		
		System.out.println("["+new Timestamp(date.getTime())+"] Classification completed successfuly.");
	}
	
	private static File createFile(InputStream input) throws IOException {
        File file = File.createTempFile("featureVector", ".csv");
        file.deleteOnExit();
        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
        	String line = reader.readLine();
            if (line == null) break;

            writer.write(line+"\n");
        }
        writer.close();
        return file;
    }
}
