import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
/*
 * Class to format ratings data & generate separate files, each containing 30 movies highly rated by
 * each user 
 * Author: Akhil Karanth
 * Edited by: Saurav Majumder
 */
public class ReviewFilesGen {
	final String bucketName = "cloudflixbucket";
	AmazonS3 s3;
	File outputFile = null;
	Writer writer = null;
	String line;
	
	public ReviewFilesGen() {
		s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
		System.out.println("Amazon S3 connection established.");
	}
	void GenOutput(String ipFile, String opFolder)
	{
		try{
			
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, ipFile));
			System.out.println("Downloading movie feature file completed.");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));

			//int stopCounter = 50;
			while((line = br.readLine()) != null) {
				//stopCounter--;
				//if(stopCounter < 1) break;
				
				StringBuilder sb=new StringBuilder();
				String parts[]=line.split("\t");
				String uid=parts[0];
				String recos=parts[1].substring(1,parts[1].length()-1);
				Pattern pattern=Pattern.compile("\\,(.*?):"); 
				Matcher matcher = pattern.matcher(recos);
				/*Split each line to get movie ids 
				 * Each line corresponds to each user
				*/
				while (matcher.find()){
					sb.append(matcher.group(1) + "\n");
				}
				
				pattern=Pattern.compile("^(.*?):");
				matcher = pattern.matcher(recos);
				if (matcher.find()) {
					sb.append(matcher.group(1) + "\n");
				}
				
				try{
					String recoData=sb.toString();
					outputFile = File.createTempFile("collabReco_"+uid, ".dat");
					outputFile.deleteOnExit();
					writer = new OutputStreamWriter(new FileOutputStream(outputFile));
					writer.write(recoData);
					writer.close();
				} catch(Exception e) {
					System.out.println(e.getMessage());
				}
				
				s3.putObject(new PutObjectRequest(bucketName, opFolder + "/collabReco_"+uid+".dat", outputFile));
				System.out.println("Uploading collorative filtering result of user:" + uid + " completed.");
			}
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReviewFilesGen gs=new ReviewFilesGen();
		gs.GenOutput("recommender_result.dat", "collaborative_result");
	}
}
