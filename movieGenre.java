/*
* ---------------------- CloudFlix: Class for movie features ------------------------
*        Author: Saurav Majumder
*        Date: Nov 14, 2013
*        Description: This is the class containing all the genre for each movie.
*        @param Input files: rating.dat.
* -----------------------------------------------------------------------------------
*/
import java.util.LinkedHashSet;
import java.util.Set;

public class movieGenre {
	protected String movie_id;
	protected String movie_name;
	protected Set<String>genre;
	
	/* Instantiate the class with movie-id and movie name */
	public movieGenre(String movie_id, String movie_name) {
		this.movie_id = movie_id;
		this.movie_name = movie_name;
		genre = new LinkedHashSet<String>();
	}
	
	/* Add genres of that movie */
	public boolean addGenre(String g) {
		{
			genre.add(g);
			return true;
		}
	}
	
	/* Return set of genre belonging to that movie */
	public Set<String> getAllGenre() {
		return genre;
	}
}
