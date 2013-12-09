/*
* ---------------------- CloudFlix: Class for movie features ------------------------
*        Author: Saurav Majumder
*        Date: Nov 14, 2013
*        Description: This is the class containing all the feature of a movie.
*        @param Input files: rating.dat.
* -----------------------------------------------------------------------------------
*/
import java.util.LinkedHashSet;
import java.util.Set;

public class movieFeature {
	protected String mId;
	protected String title;
	protected String imdbID;
	protected String year;
	protected String rtID;
	protected String rtAllCriticsRating;
	protected String rtAllCriticsNumReviews;
	protected String rtAllCriticsNumFresh;
	protected String rtAllCriticsNumRotten;
	protected String rtAllCriticsScore;
	protected String rtTopCriticsRating;
	protected String rtTopCriticsNumReviews;
	protected String rtTopCriticsNumFresh;
	protected String rtTopCriticsNumRotten;
	protected String rtTopCriticsScore;
	protected String rtAudienceRating;	
	protected String rtAudienceNumRatings;	
	protected String rtAudienceScore;
	protected String Actors;	
	protected String Directors;

	public movieFeature(String mId, String title, String imdbID, String year, String rtID, String rtAllCriticsRating, 
			String rtAllCriticsNumReviews, String rtAllCriticsNumFresh, String rtAllCriticsNumRotten, String rtAllCriticsScore, String rtTopCriticsRating, 
			String rtTopCriticsNumReviews, String rtTopCriticsNumFresh, String rtTopCriticsNumRotten, String rtTopCriticsScore, String rtAudienceRating, 
			String rtAudienceNumRatings, String rtAudienceScore, String Actors, String Directors) {
		
		this.mId = mId;
		this.title = title;
		this.imdbID = imdbID;
		this.year = year;
		this.rtID = rtID;
		this.rtAllCriticsRating = rtAllCriticsRating;
		this.rtAllCriticsNumReviews = rtAllCriticsNumReviews;
		this.rtAllCriticsNumFresh = rtAllCriticsNumFresh;
		this.rtAllCriticsNumRotten = rtAllCriticsNumRotten;
		this.rtAllCriticsScore = rtAllCriticsScore;
		this.rtTopCriticsRating = rtTopCriticsRating;
		this.rtTopCriticsNumReviews = rtTopCriticsNumReviews;
		this.rtTopCriticsNumFresh = rtTopCriticsNumFresh;
		this.rtTopCriticsNumRotten = rtTopCriticsNumRotten;
		this.rtTopCriticsScore = rtTopCriticsScore;
		this.rtAudienceRating = rtAudienceRating;	
		this.rtAudienceNumRatings = rtAudienceNumRatings;	
		this.rtAudienceScore = rtAudienceScore;
		this.Actors = Actors;	
		this.Directors = Directors;
	}
	
	public Set<String>getActorList() {
		Set<String>actor_set = new LinkedHashSet<String>();
		String[] actors = this.Actors.split("::");
		for(String actor : actors) {
			actor_set.add(actor);
		}
		return actor_set;
	}
}
