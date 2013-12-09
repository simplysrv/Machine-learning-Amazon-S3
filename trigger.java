import java.io.IOException;

public class trigger {
	public static void main(String[] args) {
		createFeatureVectorFiles user = new createFeatureVectorFiles();
		ReviewFilesGen gs=new ReviewFilesGen();
		
		try {
			// Sanitize Collaborative Filtering output.
			gs.GenOutput("recommender_result.dat", "collaborative_result");
			
			// Read all feature values.
			user.readFeatures("movie.dat", "moviesraw.dat", "sentimentResult.txt");
			
			// Create training set.
			user.createTrainingSet("ratingsForMahout.csv", "TrainingSet");
			
			// Create test set from recommender result.
			user.createTestSet("collaborative_result", "TestSet");
			
			// Perform classification.
			new movieRecommend("cloudflixbucket", "TrainingSet", "TestSet", "predictionresult", "predictionstatistics");
			
			System.out.println("[COMPLETE] All completed successfuly...");
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
}
