import requests
import json
import csv
import calendar
import operator
from datetime import datetime
from heapq import nlargest 

STATUS_CODE_OK = 200
BASE_URL = "https://api.themoviedb.org/3"
API_KEY = "insert_your_api_key_here"
CAST_Q = 4
MAX_CAST = 15
MAX_DIRECTORS = 2
TOP_N = 20
MIN_THRESHOLD = 5
MIN_THRESHOLD_FOR_DIRECTORS = 2

class FailedAPIRequest(Exception):
    """Base class for API Requests error"""
    pass

def process_API_response(response, expected_status_code):
    """Simple function to process an API response. Raises an error if the status code is not the excepected one for a correct request"""
    if response.status_code != expected_status_code:
        raise FailedAPIRequest("The call to {} failed. Expected {} status code but received {}.\n{}".format(response.url, expected_status_code, response.status_code,response.text))

class APIRequests():
    """Base object to handle all API Requests"""
    def __init__(self, api_key):
        """Params:
            api_key = String. The API key provided by TMDB to make automated requests
        """
        self.api_key = api_key

    def get_info_from_imdb_id(self, imdb_id):
        """Looks for the all the information available from an IMDB ID in the TMDB database.
        Params:
            imdb_id = String. The imdb id of the content to be searched for. The IMDB id can be obtained from the content URL.
        Returns a JSON object with the response code and content. 
        For the returned info, check https://developers.themoviedb.org/3/find/find-by-id
        """
        endpoint = "/find/" + imdb_id
        params = {"api_key": self.api_key, "external_source": "imdb_id"}
        response = requests.get(BASE_URL + endpoint, params=params)
        process_API_response(response, STATUS_CODE_OK)
        return response

    def get_movie_info(self, movie_id):
        """Gets the movie details from an TMDB ID.
        Params:
            movie_id =  String. The TMDB id of the movie.
        Returns a JSON object with the response code and content. 
        For the returned info, check https://developers.themoviedb.org/3/movies/get-movie-details
        """
        endpoint = "/movie/" + movie_id 
        # The append_to_response parameter is used to add the information about crew and cast to the response without the need to do another query
        params = {"api_key": self.api_key, "append_to_response": "credits"}
        response = requests.get(BASE_URL + endpoint, params=params)
        process_API_response(response, STATUS_CODE_OK)
        return response

    def get_show_info(self, show_id):
        """Gets the show details from an TMDB ID. 
        Params:
            show_id = String. The TMDB ID for TV show.
        Returns a JSON object with the response code and content. 
        For the returned info, check https://developers.themoviedb.org/3/tv/get-tv-details
        """
        endpoint = "/tv/" + show_id 
        # The append_to_response parameter is used to add the information about crew and cast to the response without the need to do another query
        params = {"api_key": self.api_key, "append_to_response": "credits"}
        response = requests.get(BASE_URL + endpoint, params=params)
        process_API_response(response, STATUS_CODE_OK)
        return response

    def get_episode_info(self, show_id, season, episode):
        """Gets the episode details from an TMDB ID. 
        Params:
            show_id = String. The TMDB ID for the show.
            season: String. The season number to which the episode belongs to.
            episode: String. The episode number.
        Returns a JSON object with the response code and content. 
        For the returned info, check https://developers.themoviedb.org/3/tv-episodes/get-tv-episode-details
        """
        endpoint = "/tv/" + show_id + "/season/" + season + "/episode/" + episode 
        # The append_to_response parameter is used to add the information about crew and cast to the response without the need to do another query
        params = {"api_key": self.api_key, "append_to_response": "credits"}
        response = requests.get(BASE_URL + endpoint, params=params)
        process_API_response(response, STATUS_CODE_OK)
        return response

class Content():
    def __init__(self, requester, imdb_id, is_movie):
        """Content base object. Should not be instantiate. Only instantiate the child objects as Movie and SeriesEpisode.
        Params:
            requester: APIRequests object. It's used to handle all the requests needed to populate the object with info.
            imdb_id: string. The IMDB of the content. The IMDB id can be obtained from the content URL.
            is_movie: boolean. A very rudimental way to identify what type of object to create
        """
        self._requester = requester
        self.tmdb_id = self._get_tmdb_id(imdb_id)
        # If the tmdb id wasn't found using the imdb id, i set the object in an empty state. I will later use that information 
        if self.tmdb_id:
            content_info = self._get_content_info().json()
            self.title = self._get_title(content_info)
            self.genres = self._get_genres(content_info)
            self.is_movie = is_movie
            self.is_series = not self.is_movie
            self.actors = self._get_actors(content_info)
            self.directors = self._get_directors(content_info)
            self.production_companies = self._get_production_companies(content_info)
            self.release_date = self._get_release_date(content_info)
            self.runtime = self._get_runtime(content_info)  
        else:
            self.title = None
            self.genres = None
            self.is_movie = is_movie
            self.is_series = not self.is_movie
            self.actors = None
            self.directors = None
            self.pruction_companies = None
            self.release_date = None
            self.runtime = None

    def _get_genres(self, content_info):
        """Private auxiliary method. To get the content genres, just use the attribute.
        Params:
            content_info: Json object.
        Returns a list of strings with the genres of the content.
        """
        genres = [] 
        for genre in content_info["genres"]:
            genres.append(genre["name"])
        return genres

    def _get_actors(self, content_info):
        """Private auxiliary method. To get the content actors, just use the attribute.
        Params:
            content_info: Json object.
        Returns a list of strings with the cast of the content.
        """
        actors = []
        for actor in content_info["credits"]["cast"][:MAX_CAST]:
            actors.append(actor["name"])
        return actors

    def _get_production_companies(self, content_info):
        """Private auxiliary method. To get the content production companies, just use the attribute.
        Params:
            content_info: Json object.
        Returns a list of strings with the production companies of the content.
        """
        production_companies = []
        for company in content_info["production_companies"]:
            production_companies.append(company["name"])
        return production_companies


class Movie(Content):
    def __init__(self, requester, imdb_id):
        """Params:
            requester: APIRequests object. It's used to handle all the requests needed to populate the object with info.
            imdb_id: string. The IMDB of the content. The IMDB id can be obtained from the content URL.
        Public attributes: check the Content object constructor.
        """
        # The "True" is passed for the "is_movie" attribute.
        Content.__init__(self, requester, imdb_id, True)

    def _get_tmdb_id(self, imdb_id):
        """Private auxiliary method to get the TMDB id. To use it, just call the object's attribute.
        Params:
            imdb_id = String. The IMDB ID of the movie.
        Returns a string with the tmdb id
        """
        movie_info = self._requester.get_info_from_imdb_id(imdb_id).json()["movie_results"]
        # No results found for that imdb id in the tmdb database
        if len(movie_info) == 0:
            return None
        # Since imdb ids are unique, this list shouldn't have more than one element.
        else:
            return str(movie_info[0]["id"])

    def _get_content_info(self):
        """Private auxiliary method to get the movie info that is going to be used to populate the attributes.
        Returns a JSON object with the response code and content. 
        For the returned info, check https://developers.themoviedb.org/3/movies/get-movie-details
        """
        return self._requester.get_movie_info(self.tmdb_id)

    def _get_title(self, content_info):
        """Private auxiliary method. To get the title, just use the object's attribute.
        Params:
            content_info: Json object.
        Returns a string with the original movie title.
        """
        return content_info["original_title"]

    def _get_directors(self, content_info):
        """Private auxiliary method. To get the directors, just use the object' attribute.
        Params:
            content_info: Json object.
        Returns a list of string with the movie directors.
        """
        directors = []
        for crew in content_info["credits"]["crew"]:
            if crew["job"] != "Director":
                continue
            else:
                directors.append(crew["name"])
        return directors[:MAX_DIRECTORS]

    def _get_release_date(self, content_info):
        """Private auxiliary method. To get the release date, just use the object's attribute.
        Params:
            content_info: Json object.
        Returns a date object with the movie's release date.
        """
        year, month, day = content_info["release_date"].split("-")
        return datetime(int(year), int(month), int(day))

    def _get_runtime(self, content_info):
        """Private auxiliary method. To get the runtime, just use the objects attribute.
        Params:
            content_info: Json object.
        Returns an int that represents the movie's runtime in minutes.
        """
        return int(content_info["runtime"])
            

class SeriesEpisode(Content):
    def __init__(self, requester, imdb_id):
        """Params:
            requester: APIRequests object. It's used to handle all the requests needed to populate the object with info.
            imdb_id: string. The IMDB of the content. The IMDB id can be obtained from the content URL.
        Public attributes: check the Content object constructor. show_id (string), season (string) and episode (string) are also added.
        """
        self.show_id = None
        self.season = None
        self.episode = None
        # The "False" is passed for the "is_movie" attribute.
        Content.__init__(self, requester, imdb_id, False)

    def _get_tmdb_id(self, imdb_id):
        """Private auxiliary method to get the TMDB id. To use it, just call the object's attribute.
        Params:
            imdb_id = String. The IMDB ID of the movie.
        Returns a string with the tmdb id. It also populates the show_id, season and episode attributes.
        """
        episode_info = self._requester.get_info_from_imdb_id(imdb_id).json()["tv_episode_results"]
        # No results found for that imdb id in the tmdb database
        if len(episode_info) == 0:
            self.show_id = None
            self.season = None
            self.episode = None
            return None
        # Since imdb ids are unique, this list shouldn't have more than one element.
        else:
            self.show_id = str(episode_info[0]["show_id"])
            self.season = str(episode_info[0]["season_number"])
            self.episode = str(episode_info[0]["episode_number"])
            return str(episode_info[0]["id"])

    def _get_content_info(self):
        """Private auxiliary method to get the show info that is going to be used to populate the attributes.
        Returns a JSON object with the response code and content. 
        For the returned info, check https://developers.themoviedb.org/3/movies/tv-details
        """
        return self._requester.get_show_info(self.show_id)

    def _get_title(self, content_info):
        """Private auxiliary method. To get the title, just use the object's attribute.
        Params:
            content_info: Json object.
        Returns a string with the original show title.
        """
        return content_info["original_name"]

    def _get_directors(self, content_info):
        """Private auxiliary method. To get the directors, just use the object' attribute.
        Params:
            content_info: Json object.
        Returns a list of strings with the episode's directors.
        """
        content_info = self._requester.get_episode_info(self.show_id, self.season, self.episode).json()
        directors = []
        for crew in content_info["credits"]["crew"]:
            if crew["job"] != "Director":
                continue
            else:
                directors.append(crew["name"])
        return directors[:MAX_DIRECTORS]

    def _get_release_date(self, content_info):
        """Private auxiliary method. To get the release date, just use the object's attribute.
        Params:
            content_info: Json object.
        Returns a date object with the episode's release date.
        """
        content_info = self._requester.get_episode_info(self.show_id, self.season, self.episode).json()
        year, month, day = content_info["air_date"].split("-")
        return datetime(int(year), int(month), int(day))

    def _get_runtime(self, content_info):
        """Private auxiliary method. To get the runtime, just use the objects attribute.
        Params:
            content_info: Json object.
        Returns an int that represents the episode's runtime in minutes.
        """
        # Episodes don't have individual runtime on the API, so this is a work around. I just do an average beteween all the values
        # that are on show's runtime attribute.
        return int(sum(content_info["episode_run_time"])/len(content_info["episode_run_time"]))


class WatchedContent():
    def __init__(self, content, platform, date, rating):
        """ Represents a watched content. 
        Params and attributes:
            content: Either a Movie or a SeriesEpisode object.
            platform: String. Represents where you've watched the content.
            date: Date object. Represents the date when you've watched the content.
            rating: Float. Represents how much you liked the content
        """
        self.content = content
        self.platform = platform
        self.date = date
        self.rating = rating

def load_watching_habits_to_memory_from_csv(file_name):
    """Params:
        file_name: string with the name of the file with the watching habits info.
        The file should have the following columnd: Date, Movie or Series, Name, Platform, Rating, IMDB ID
        The file should use tab as the column delimiter
    Returns a list of Dictionaries with all the content watched
    """
    with open(file_name, "r") as f:
        f_csv = csv.DictReader(f, delimiter="\t")
        ret_value = []
        for line in f_csv:
            tmp = {}
            for key, value in line.items():
                tmp[key] = value
            ret_value.append(tmp)
    return ret_value

def get_most_watched_genres(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most watched genres.
    """
    scores = {}
    for c in watched_content:
        for genre in c.content.genres:
            if genre not in scores:
                scores[genre] = 0
            scores[genre] += c.content.runtime
    print("Most watched genres")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))

def get_most_liked_genres(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most liked genres.
    """
    ratings = {}
    amount_watched = {}
    scores = {}
    for c in watched_content:
        for genre in c.content.genres:
            if genre not in ratings:
                ratings[genre] = 0
                amount_watched[genre] = 0
            ratings[genre] += c.rating
            amount_watched[genre] += 1
    for genre, rating in ratings.items():
        # The treshold is in place to avoid "one hit wonders" that may get a very high score
        if amount_watched[genre] < MIN_THRESHOLD:
            continue
        scores[genre] = float(rating) / amount_watched[genre]

    print("Best rated genres")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))

def get_amount_of_movies_and_series_watched(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the amount of movies and series episodes watched.
    """
    movie_counter = 0
    movie_minutes = 0
    series_counter = 0
    series_minutes = 0
    for c in watched_content:
        if c.content.is_movie:
            movie_counter += 1
            movie_minutes += c.content.runtime
        else:
            series_counter += 1
            series_minutes += c.content.runtime
    print("Amount of movies watched: {} ({} minutes)".format(movie_counter, movie_minutes))
    print("Amount of series watched: {} ({} minutes)".format(series_counter, series_minutes))

def get_platform_usage(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the usage of each platform.
    """
    counter = {}
    for c in watched_content:
        if c.platform not in counter:
            counter[c.platform] = 0
        counter[c.platform] += c.content.runtime
    print("Platform usage")
    for platform, count in counter.items():
        print("{}: {}".format(platform, count))

def get_most_watched_actors(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most watched actors.
    """
    scores = {}
    for c in watched_content:
        for actor in c.content.actors:
            if actor not in scores:
                scores[actor] = 0
            scores[actor] += c.content.runtime
    print("Most watched actors")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))

def get_most_liked_actors(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most liked actors.
    """
    ratings = {}
    amount_watched = {}
    scores = {}
    for c in watched_content:
        for actor in c.content.actors:
            if actor not in ratings:
                ratings[actor] = 0
                amount_watched[actor] = 0
            ratings[actor] += c.rating
            amount_watched[actor] += 1
    for actor, rating in ratings.items():
        # The treshold is in place to avoid "one hit wonders" that may get a very high score
        if amount_watched[actor] < MIN_THRESHOLD:
            continue
        scores[actor] = float(rating) / amount_watched[actor] 
    print("Best rated actor")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))


def get_most_watched_production_companies(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most watched production companies.
    """
    scores = {}
    for c in watched_content:
        for p_d in c.content.production_companies:
            if p_d not in scores:
                scores[p_d] = 0
            scores[p_d] += c.content.runtime
    print("Most watched production_companies")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))

def get_most_liked_production_companies(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most liked production companies.
    """
    ratings = {}
    amount_watched = {}
    scores = {}
    for c in watched_content:
        for p_d in c.content.production_companies:
            if p_d not in ratings:
                ratings[p_d] = 0
                amount_watched[p_d] = 0
            ratings[p_d] += c.rating
            amount_watched[p_d] += 1
    for p_d, rating in ratings.items():
        # The treshold is in place to avoid "one hit wonders" that may get a very high score
        if amount_watched[p_d] < MIN_THRESHOLD:
            continue
        scores[p_d] = float(rating) / amount_watched[p_d] 
    print("Best rated production companies")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))


def get_most_watched_directors(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most wacthed directors.
    """
    scores = {}
    for c in watched_content:
        for director in c.content.directors:
            if director not in scores:
                scores[director] = 0
            scores[director] += c.content.runtime
    print("Most watched directors")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))

def get_most_liked_directors(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the most liked directors.
    """
    ratings = {}
    amount_watched = {}
    scores = {}
    for c in watched_content:
        for director in c.content.directors:
            if director not in ratings:
                ratings[director] = 0
                amount_watched[director] = 0
            ratings[director] += c.rating
            amount_watched[director] += 1
    for director, rating in ratings.items():
        # The treshold is in place to avoid "one hit wonders" that may get a very high score
        if amount_watched[director] < MIN_THRESHOLD_FOR_DIRECTORS:
            continue
        scores[director] = float(rating) / amount_watched[director] 
    print("Best rated director")
    for x in nlargest(TOP_N, scores, key = scores.get):
        print("{}: {}".format(x, scores[x]))

def get_activity_by_month_and_day(watched_content):
    """Params: 
        watched_content: a list of WatchedContent objects
    Returns nothing. Prints the activity bv mont, the activity by weekday and the day with the most activity.
    """
    months = {"January": 0, "February": 0, "March": 0, "April": 0, "May": 0, "June": 0, "July": 0, "August": 0, "September": 0, "October": 0, "November": 0, "December": 0}
    weekday = {"Sunday": 0, "Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0, "Friday": 0, "Saturday": 0}
    days = {}
    for c in watched_content:
        months[calendar.month_name[c.date.month]] += c.content.runtime
        weekday[calendar.day_name[c.date.weekday()]] += c.content.runtime
        if c.date not in days:
            days[c.date] = 0
        days[c.date] += c.content.runtime
    print("Activity by month")
    for month, amount in months.items():
        print("{}: {}".format(month, amount))
    print()
    print("Activity by weekday")
    for day, amount in weekday.items():
        print("{}: {}".format(day, amount))
    print()
    max_day = max(days.items(), key=operator.itemgetter(1))[0]
    print("Day with the most activity: {}/{}".format(max_day.month, max_day.day))


def main():
    tmdb = APIRequests(API_KEY)
    watching_habits_raw = load_watching_habits_to_memory_from_csv("Watching habits.tsv")
    watched_content = []
    # Building the list of watched content
    for content in watching_habits_raw:
        if content["Movie or Series"]== "Movie":
            c = Movie(tmdb, content["IMDB ID"])
        else:
            c = SeriesEpisode(tmdb, content["IMDB ID"])
        if c.tmdb_id == None:
            print("There's an error with {} ({})".format(content["Name"], content["IMDB ID"]))
            continue
        month, day, year = content["Date"].split("/")
        watched_content.append(WatchedContent(c, content["Platform"], datetime(int(year), int(month), int(day)), float(content["Rating"])))

    get_most_watched_genres(watched_content)
    print()
    get_most_liked_genres(watched_content)
    print()
    get_amount_of_movies_and_series_watched(watched_content)
    print()
    get_platform_usage(watched_content)
    print()
    get_most_watched_actors(watched_content)
    print()
    get_most_liked_actors(watched_content)
    print()
    get_most_watched_production_companies(watched_content)
    print()
    get_most_liked_production_companies(watched_content)
    print()
    get_most_watched_directors(watched_content)
    print()
    get_most_liked_directors(watched_content)
    print()
    get_activity_by_month_and_day(watched_content)

main()
