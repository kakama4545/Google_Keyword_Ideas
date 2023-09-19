from flask import Flask, request, jsonify
import difflib, os, re, requests
from flask_mysqldb import MySQL
from googlesearcher import Google
from urllib.parse import urlparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pytrends.request import TrendReq
import cachetools, requests, time

# Increase the timeout for all requests made by the `requests` library
# This should be placed at the beginning of your script
requests.adapters.DEFAULT_RETRIES = 10
session = requests.Session()
session.mount('https://', requests.adapters.HTTPAdapter(max_retries=10))

load_dotenv()

app = Flask(__name__)

# MySQL configurations
app.config['MYSQL_HOST'] = os.getenv('MYSQL_HOST')
app.config['MYSQL_USER'] = os.getenv('MYSQL_USER')
app.config['MYSQL_PASSWORD'] = os.getenv('MYSQL_PASSWORD')
app.config['MYSQL_DB'] = os.getenv('MYSQL_DB')
app.config['MYSQL_PORT'] = 3306
app.config['MYSQL_SSL'] = os.getenv('MYSQL_SSL')
mysql = MySQL(app)

# RapidAPI configurations
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
RAPIDAPI_HOST = 'targeted-keyword-trend.p.rapidapi.com'

# Define a list of allowed country codes
allowed_countries = ['us', 'uk', 'ca', 'in']  # Add more as needed

# Define the initial date and update interval
date_present = datetime(2023, 8, 25)
update_interval = timedelta(days=10)

# Function to update the date cyclically
def update_date_cyclically(current_date, interval):
    updated_date = current_date + interval
    if updated_date.year > current_date.year:
        return current_date.replace(year=updated_date.year)
    return updated_date
# Calculate the "Updated" date cyclically
updated_date = update_date_cyclically(date_present, update_interval)
formatted_date = updated_date.strftime('%B %d, %Y')

# Initialize a simple cache with a maximum size (adjust as needed)
cache = cachetools.LRUCache(maxsize=100)

# Define the minimum delay between requests (in seconds)
min_request_delay = 5  # Adjust this as needed

# Initialize the TrendReq object outside of the functions
pytrends = TrendReq(hl='en-US', tz=360)

def make_google_request(keyword, country_code):
    try:
        request_key = f"{keyword}_{country_code}"

        # Check if the request is already cached
        if request_key in cache:
            print("Using cached data...")
            return cache[request_key]

        # Initialize the TrendReq object inside the function
        pytrends = TrendReq(hl='en-US', tz=360, geo=country_code.upper())

        # Make your Google request here using pytrends or any other method you prefer
        # Specify the keywords you want to query in a list
        keywords = [keyword]

        # Build payload with the specified keywords
        pytrends.build_payload(kw_list=keywords)

        # Get related queries using pytrends
        data = pytrends.related_queries()

        # Add a delay to respect rate limits
        time.sleep(min_request_delay)

        # Cache the response data
        cache[request_key] = data

        return data

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def fetch_google_trends_data(keyword, country_code):
    try:
        # Initialize the TrendReq object
        
        # Use the make_google_request function to make the request
        data = make_google_request(keyword, country_code)

        # Create a dictionary to store the formatted related queries
        formatted_queries = {}

        # Check if the keyword exists in the data
        if keyword in data:
            keyword_data = data[keyword]

            # Format top queries data
            top_queries_df = keyword_data.get('top', None)
            if top_queries_df is not None:
                top_queries_list = [{"keyword idea": row['query'], "search value": row['value']} for _, row in top_queries_df.iterrows()]
            else:
                top_queries_list = []

            # Format rising queries data
            rising_queries_df = keyword_data.get('rising', None)
            if rising_queries_df is not None:
                rising_queries_list = [{"keyword idea": row['query'], "search value": row['value']} for _, row in rising_queries_df.iterrows()]
            else:
                rising_queries_list = []

            # Store the formatted data
            formatted_queries[keyword] = {
                'top': top_queries_list,
                'rising': rising_queries_list
            }
        else:
            formatted_queries[keyword] = {
                'top': [],
                'rising': []
            }

        # Format the data into a list with a single dictionary
        formatted_data = [{"related keywords shown on Google Trends": formatted_queries}]

        return formatted_data

    except Exception as e:
        return {"error": str(e)}

# Modify the fetch_interest_by_region_data function
def fetch_interest_by_region_data(keyword, country_code):
    try:
        # Initialize the TrendReq object
        pytrends = TrendReq(hl='en-US', tz=360, geo=country_code.upper())

        # Specify the keyword you want to query
        keywords = [keyword]

        # Build payload with the specified keyword
        pytrends.build_payload(kw_list=keywords)

        # Get "Interest by Region" data
        interest_by_region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_geo_code=False)

        # Convert the data to a list of dictionaries
        interest_data_list = []
        for index, row in interest_by_region_data.iterrows():
            interest_data_list.append({"region": index, "interest": int(row[keyword])})  # Convert interest to int

        # Format the data into a dictionary
        formatted_data = {"Interest Google Trends Data": interest_data_list}

        return formatted_data

    except Exception as e:
        # Handle exceptions gracefully
        return {"Interest Google Trends Data": []}
    
def fetch_country_serp_data(sanitized_keyword, sanitized_country, num_results=10):
    try:
        #ssanitized_country= "us"
        request_key = f"{sanitized_keyword}_{num_results}_{sanitized_country}"

        # Check if the request is already cached
        if request_key in cache:
            print("Using cached data...")
            return cache[request_key]

        # Apply rate limiting and time delay
        if 'last_request_time' in cache:
            last_request_time = cache['last_request_time']
            elapsed_time = time.time() - last_request_time
            if elapsed_time < min_request_delay:
                sleep_time = min_request_delay - elapsed_time
                print(f"Sleeping for {sleep_time} seconds to respect rate limits...")
                time.sleep(sleep_time)

        # Specify the search query and the country code
        query = f"{sanitized_keyword} country:{sanitized_country}"

        start_position = request.args.get('start', default='1')
        num_results = request.args.get('num', default='10')

        start_position = int(start_position) - 1  # Adjust to 0-based index
        results = Google.search(query, num=num_results)

        end_position = min(start_position + int(num_results), len(results))

        formatted_results = []

        for i in range(start_position, end_position):
            result = results[i]
            parsed_url = urlparse(result.link)
            domain = parsed_url.netloc
            formatted_results.append({
                'position': i + 1,
                'link': result.link,
                'title': result.title,
                'domain': domain
            })

        serp_analysis = {
            "Date": datetime.today().strftime('%B %d, %Y'),
            "Number of SERP Results": len(results),
            "SERP Results (Top 100)": formatted_results
        }

        # Update the last_request_time in the cache
        cache['last_request_time'] = time.time()

        # Cache the response data
        cache[request_key] = serp_analysis

        return serp_analysis

    except Exception as e:
        return {'error': str(e)}


    
# Route to fetch keyword data and SERP data from the MySQL database
@app.route('/keyword_overview_Data', methods=['GET'])
def get_keyword_data():
    try:
        keyword = request.args.get('keyword')
        country = request.args.get('country')

        # Check if keyword and country are missing or invalid
        if not keyword or not re.match(r'^[a-zA-Z\s]+$', keyword) or not country:
            return jsonify({"error": "Invalid keyword or country parameter."}), 400
        if not keyword:
            return jsonify({"error": "Keyword parameter is missing."}), 400

        sanitized_keyword = keyword.strip().lower()

        # Define a list of allowed country codes
        allowed_countries = ['us', 'uk', 'ca', 'in']  # Add more as needed

        # Check if the provided country code is in the list of allowed countries
        if country.lower() not in allowed_countries:
            return jsonify({"error": "Invalid country code."}), 400
        
        sanitized_country = country.strip().lower()
        # Call the fetch_country_serp_data function with the correct parameter names
        serp_analysis = fetch_country_serp_data(sanitized_keyword, sanitized_country, num_results=10)

        # Fetch Google Trends data
        google_trends_data = fetch_google_trends_data(sanitized_keyword, sanitized_country)
        # Append Interest by Region data to the response
        interest_data = fetch_interest_by_region_data(sanitized_keyword, sanitized_country)
         # Create a MySQL connection
        db_connection = mysql.connection
        cursor = db_connection.cursor()
        
    
        # Construct the table name based on the selected country
        table_name = f'google_keyword_data_{sanitized_country.lower()}'

        # Use string formatting to insert the table name into the SQL query
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        keywords_data = cursor.fetchall()

        # Use a parameterized query to select data from the table
        #query = 'SELECT * FROM %s'
        #cursor.execute(query, (table_name,))
        #keywords_data = cursor.fetchall()

        exact_match_data = None
        related_data = []

        for keyword_data in keywords_data:
           
            if keyword_data and isinstance(keyword_data[0], str):  # Assuming 'Keyword' is the second column
                keyword_lower = keyword_data[0].strip().lower()  # Update index if needed
                
                if sanitized_keyword == keyword_lower:
                    #print (sanitized_keyword == keyword_lower)
                    selected_data = {
                        "Keyword": keyword_data[0],  # Replace with appropriate column index
                    "Avg. monthly searches": keyword_data[1],  # Replace with appropriate column index
                    "Competition": keyword_data[2],  # Replace with appropriate column index
                    "Competition (indexed value)": keyword_data[3],  # Replace with appropriate column index
                    "Top of page bid (low range)": keyword_data[4],  # Replace with appropriate column index
                    "Top of page bid (high range)": keyword_data[5],  # Replace with appropriate column index
                    "Searches: Sep 2022": keyword_data[6],  # Replace with appropriate column index
                    "Searches: Oct 2022": keyword_data[7],  # Replace with appropriate column index
                    "Searches: Nov 2022": keyword_data[8],  # Replace with appropriate column index
                    "Searches: Dec 2022": keyword_data[9],  # Replace with appropriate column index
                    "Searches: Jan 2023": keyword_data[10],  # Replace with appropriate column index
                    "Searches: Feb 2023": keyword_data[11],  # Replace with appropriate column index
                    "Searches: Mar 2023": keyword_data[12],  # Replace with appropriate column index
                    "Searches: Apr 2023": keyword_data[13],  # Replace with appropriate column index
                    "Searches: May 2023": keyword_data[14],  # Replace with appropriate column index
                    "Searches: Jun 2023": keyword_data[15],  # Replace with appropriate column index
                    "Searches: Jul 2023": keyword_data[16],  # Replace with appropriate column index
                    "Searches: Aug 2023": keyword_data[17],  # Replace with appropriate column index
                    "Updated": formatted_date  # Use the calculated date from one week ago
                    }
                    exact_match_data = selected_data
                    
                    break  # Exit the loop after finding an exact match

        for data in keywords_data:
            if data and isinstance(data[0], str):  # Assuming 'Keyword' is the second column
                
                similarity_ratio = difflib.SequenceMatcher(None, sanitized_keyword, data[0].strip().lower()).ratio()
                if similarity_ratio >= 0.8 and len(data[0].strip()) > 2:  # Exclude very short words
                    related_data.append(data)

        total_related_keywords = len(related_data)
        #print(related_data)
        #Fetch SERP data
        """try:
            start_position = request.args.get('start', default='1')
            num_results = request.args.get('num', 'us', default='1')

            start_position = int(start_position) - 1  # Adjust to 0-based index
            results = Google.search(keyword, num=num_results)

            end_position = min(start_position + int(num_results), len(results))

            formatted_results = []
            #print(formatted_results)
            for i in range(start_position, end_position):
                result = results[i]
                parsed_url = urlparse(result.link)
                domain = parsed_url.netloc
                formatted_results.append({
                    'position': i + 1,
                    'link': result.link,
                    'title': result.title,
                    'domain': domain
                })

            serp_analysis = {
                "Date": datetime.today().strftime('%B %d, %Y'),
                "Number of SERP Results": len(results),
                "SERP Results (Top 100)": formatted_results
            }

        except Exception as e:
            serp_analysis = {'error': str(e)}"""

        # Fetch RapidAPI data
        try:
            rapidapi_url = f"https://targeted-keyword-trend.p.rapidapi.com/{keyword}"  # Replace {keyword} with the actual keyword parameter
            headers = {
                "X-RapidAPI-Key": RAPIDAPI_KEY,
                "X-RapidAPI-Host": "targeted-keyword-trend.p.rapidapi.com"
            }
            response = requests.get(rapidapi_url, headers=headers)
            data = response.json()
            # Extract 'Month_Date_Year' and 'Search_Count' from each dictionary
            formatted_data = []
            
            for item in data:
               # print (data)
                if isinstance(item, dict):
                   month_date_year = item.get('Month_Date_Year')
                   search_count = item.get('Search_Count')
                   
                   formatted_data.append({'Month_Date_Year': month_date_year, 'Search_Count': search_count})

            form_data = []
            for entry in formatted_data:
               # Check if Month_Date_Year is None
                if month_date_year is None:
                    #return jsonify({"error": "Month_Date_Year is missing in the data."}), 400
                     form_data.append({"error": "Month_Date_Year is missing in the data."})
                else :
                    form_data.append({'Month_Date_Year': entry["Month_Date_Year"], 'Search_Count': entry["Search_Count"]})
        except Exception as e:
            form_data = {'error': str(e)}
            
        response = []

        if exact_match_data:
            response.append({"Keyword Overview": exact_match_data})
        else:
            response.append({"Keyword Overview": "No data found in the database for Keyword Overview."})

        # Filter and append only closely related keywords
        filtered_related_data = []
        for keyword_data in related_data:
            
            keyword = keyword_data[0].strip().lower()
            
            similarity_ratio = difflib.SequenceMatcher(None, sanitized_keyword, keyword).ratio()
            #if similarity_ratio >= 0.8 and len(keyword) > 2:  # Exclude very short words
            filtered_related_data.append({
                    "Keyword": keyword_data[0],  # Replace with appropriate column index
                    "Avg. monthly searches": keyword_data[1],  # Replace with appropriate column index
                    "Competition": keyword_data[2],  # Replace with appropriate column index
                    "Competition (indexed value)": keyword_data[3],  # Replace with appropriate column index
                    "Top of page bid (low range)": keyword_data[4],  # Replace with appropriate column index
                    "Top of page bid (high range)": keyword_data[5],  # Replace with appropriate column index
                    "Searches: Sep 2022": keyword_data[6],  # Replace with appropriate column index
                    "Searches: Oct 2022": keyword_data[7],  # Replace with appropriate column index
                    "Searches: Nov 2022": keyword_data[8],  # Replace with appropriate column index
                    "Searches: Dec 2022": keyword_data[9],  # Replace with appropriate column index
                    "Searches: Jan 2023": keyword_data[10],  # Replace with appropriate column index
                    "Searches: Feb 2023": keyword_data[11],  # Replace with appropriate column index
                    "Searches: Mar 2023": keyword_data[12],  # Replace with appropriate column index
                    "Searches: Apr 2023": keyword_data[13],  # Replace with appropriate column index
                    "Searches: May 2023": keyword_data[14],  # Replace with appropriate column index
                    "Searches: Jun 2023": keyword_data[15],  # Replace with appropriate column index
                    "Searches: Jul 2023": keyword_data[16],  # Replace with appropriate column index
                    "Searches: Aug 2023": keyword_data[17],  # Replace with appropriate column index
                    "Updated": formatted_date  # Use the calculated date from one week ago
                })
            #print (filtered_related_data)
        if filtered_related_data:
            filtered_related_data.insert(0, {"Total Related Keywords": len(filtered_related_data)})
            response.append({"Related Keywords": filtered_related_data})
            response.append({"Your Targeted Keword Trending History On Google": form_data })  # Add RapidAPI data to the response
            response.append({"SERP Analysis": serp_analysis})
            # Append Google Trends data to the response
            response.append({"Google Trends Data": google_trends_data})
            response.append({"Interest Google Trends Data": interest_data})
            fulldata = "Keyword Overview full analysis with SERP Analysis, Keword Trending History, related Queries and keyword intrest by region On Google Trends, Related Keywords data."
            response.append({"Comprehensive Keyword Analysis": fulldata })

        else :
            response.append({"Related Keywords": "No closely related keywords found in the database."})
            response.append({"Your Targeted Keword Trending History On Google": "No closely Your Targeted Keword Trending History On Google" })  # Add RapidAPI data to the response
            response.append({"SERP Analysis": "Sorry serp_analysis is not found. Please try after some time"})
   
        ordered_response = [
             response_item
            for name in ["Comprehensive Keyword Analysis", "Keyword Overview", "SERP Analysis", "Your Targeted Keword Trending History On Google", "Interest Google Trends Data", "Google Trends Data", "Total Related Keywords", "Related Keywords"]
           if (response_item := next((item for item in response if name in item), None))
           
     ]
        # Close the database connection
        cursor.close()

        # Return the response
        return jsonify(ordered_response)

    except Exception as e:
        datae = {"message": "Sorry.. No data found in my Database. if you try after 1 min, get big Keyword Research Data... or try another country"}
        return jsonify({'error': str(datae)}), 200
    
   # Route to fetch keyword data and SERP data from the MySQL database
@app.route('/keyword_ideas', methods=['GET'])
def semrush_keyword_Data():
    try:
        keyword = request.args.get('keyword')
        country = request.args.get('country')

        # Check if keyword and country are missing or invalid
        if not keyword or not re.match(r'^[a-zA-Z\s]+$', keyword) or not country:
            return jsonify({"error": "Invalid keyword or country parameter."}), 400
        if not keyword:
            return jsonify({"error": "Keyword parameter is missing."}), 400

        sanitized_keyword = keyword.strip().lower()

        # Define a list of allowed country codes
        allowed_countries = ['us', 'uk', 'ca', 'in']  # Add more as needed

        # Check if the provided country code is in the list of allowed countries
        if country.lower() not in allowed_countries:
            return jsonify({"error": "Invalid country code."}), 400
        
        sanitized_country = country.strip().lower()

         # Create a MySQL connection
        db_connection = mysql.connection
        cursor = db_connection.cursor()
    
        # Construct the table name based on the selected country
        table_name = f'google_keyword_data_{sanitized_country.lower()}'

        # Use string formatting to insert the table name into the SQL query
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        keywords_data = cursor.fetchall()
        if not table_name:
            # Handle the case where no data is found in the table
            return jsonify({"message": "No data found in the database."})

        response = []

         # Filter and append only closely related keywords
        filtered_related_data = []
        for keyword_data in keywords_data:
            keyword = keyword_data[0].strip().lower()
            similarity_ratio = difflib.SequenceMatcher(None, sanitized_keyword, keyword).ratio()
            if similarity_ratio >= 0.8:  # Exclude very short words
                filtered_related_data.append({
                    "Keyword": keyword_data[0],  # Replace with appropriate column index
                    "Avg. monthly searches": keyword_data[1],  # Replace with appropriate column index
                    "Competition": keyword_data[2],  # Replace with appropriate column index
                    "Competition (indexed value)": keyword_data[3],  # Replace with appropriate column index
                    "Top of page bid (low range)": keyword_data[4],  # Replace with appropriate column index
                    "Top of page bid (high range)": keyword_data[5],  # Replace with appropriate column index
                    "Searches: Sep 2022": keyword_data[6],  # Replace with appropriate column index
                    "Searches: Oct 2022": keyword_data[7],  # Replace with appropriate column index
                    "Searches: Nov 2022": keyword_data[8],  # Replace with appropriate column index
                    "Searches: Dec 2022": keyword_data[9],  # Replace with appropriate column index
                    "Searches: Jan 2023": keyword_data[10],  # Replace with appropriate column index
                    "Searches: Feb 2023": keyword_data[11],  # Replace with appropriate column index
                    "Searches: Mar 2023": keyword_data[12],  # Replace with appropriate column index
                    "Searches: Apr 2023": keyword_data[13],  # Replace with appropriate column index
                    "Searches: May 2023": keyword_data[14],  # Replace with appropriate column index
                    "Searches: Jun 2023": keyword_data[15],  # Replace with appropriate column index
                    "Searches: Jul 2023": keyword_data[16],  # Replace with appropriate column index
                    "Searches: Aug 2023": keyword_data[17],  # Replace with appropriate column index
                    "Updated": formatted_date  # Use the calculated date from one week ago
               })

        if filtered_related_data:
            filtered_related_data.insert(0, {"Total Related Keywords": len(filtered_related_data)})
            response.append({"Related Keywords": filtered_related_data})
        else:
            response.append({"Related Keywords": "No closely related keywords found in the database."})

        
        ordered_response = [
            response_item
            for name in ["Total Related Keywords", "Related Keywords"]
            if (response_item := next((item for item in response if name in item), None))
        ]

        # Close the database connection
        cursor.close()

        # Return the response
        return jsonify(ordered_response)

    except Exception as e:
        datae = {"message": "Sorry.. No data found in my Database. if you try after 1 min, get big Keyword Research Data... or try another country"}
        return jsonify({'error': str(datae)}), 200

# Route to fetch keyword data and SERP data from the MySQL database
@app.route('/google_keyword_overview', methods=['GET'])
def get_google_keyword_data():
    try:
        keyword = request.args.get('keyword')
        country = request.args.get('country')

        # Check if keyword and country are missing or invalid
        if not keyword or not re.match(r'^[a-zA-Z\s]+$', keyword) or not country:
            return jsonify({"error": "Invalid keyword or country parameter."}), 400
        if not keyword:
            return jsonify({"error": "Keyword parameter is missing."}), 400

        sanitized_keyword = keyword.strip().lower()

        # Define a list of allowed country codes
        allowed_countries = ['us', 'uk', 'ca', 'in']  # Add more as needed

        # Check if the provided country code is in the list of allowed countries
        if country.lower() not in allowed_countries:
            return jsonify({"error": "Invalid country code."}), 400
        
        sanitized_country = country.strip().lower()
        # Call the fetch_country_serp_data function with the correct parameter names
        serp_analysis = fetch_country_serp_data(sanitized_keyword, sanitized_country, num_results=10)

        # Fetch Google Trends data
        google_trends_data = fetch_google_trends_data(sanitized_keyword, sanitized_country)
        # Append Interest by Region data to the response
        interest_data = fetch_interest_by_region_data(sanitized_keyword, sanitized_country)
         # Create a MySQL connection
        db_connection = mysql.connection
        cursor = db_connection.cursor()
        
    
        # Construct the table name based on the selected country
        table_name = f'google_keyword_data_{sanitized_country.lower()}'

        # Use string formatting to insert the table name into the SQL query
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        keywords_data = cursor.fetchall()

        # Use a parameterized query to select data from the table
        #query = 'SELECT * FROM %s'
        #cursor.execute(query, (table_name,))
        #keywords_data = cursor.fetchall()

        exact_match_data = None
        related_data = []

        for keyword_data in keywords_data:
           
            if keyword_data and isinstance(keyword_data[0], str):  # Assuming 'Keyword' is the second column
                keyword_lower = keyword_data[0].strip().lower()  # Update index if needed
                
                if sanitized_keyword == keyword_lower:
                    #print (sanitized_keyword == keyword_lower)
                    selected_data = {
                        "Keyword": keyword_data[0],  # Replace with appropriate column index
                    "Avg. monthly searches": keyword_data[1],  # Replace with appropriate column index
                    "Competition": keyword_data[2],  # Replace with appropriate column index
                    "Competition (indexed value)": keyword_data[3],  # Replace with appropriate column index
                    "Top of page bid (low range)": keyword_data[4],  # Replace with appropriate column index
                    "Top of page bid (high range)": keyword_data[5],  # Replace with appropriate column index
                    "Searches: Sep 2022": keyword_data[6],  # Replace with appropriate column index
                    "Searches: Oct 2022": keyword_data[7],  # Replace with appropriate column index
                    "Searches: Nov 2022": keyword_data[8],  # Replace with appropriate column index
                    "Searches: Dec 2022": keyword_data[9],  # Replace with appropriate column index
                    "Searches: Jan 2023": keyword_data[10],  # Replace with appropriate column index
                    "Searches: Feb 2023": keyword_data[11],  # Replace with appropriate column index
                    "Searches: Mar 2023": keyword_data[12],  # Replace with appropriate column index
                    "Searches: Apr 2023": keyword_data[13],  # Replace with appropriate column index
                    "Searches: May 2023": keyword_data[14],  # Replace with appropriate column index
                    "Searches: Jun 2023": keyword_data[15],  # Replace with appropriate column index
                    "Searches: Jul 2023": keyword_data[16],  # Replace with appropriate column index
                    "Searches: Aug 2023": keyword_data[17],  # Replace with appropriate column index
                    "Updated": formatted_date  # Use the calculated date from one week ago
                    }
                    exact_match_data = selected_data
                    
                    break  # Exit the loop after finding an exact match

        for data in keywords_data:
            if data and isinstance(data[0], str):  # Assuming 'Keyword' is the second column
                
                similarity_ratio = difflib.SequenceMatcher(None, sanitized_keyword, data[0].strip().lower()).ratio()
                if similarity_ratio >= 0.8 and len(data[0].strip()) > 2:  # Exclude very short words
                    related_data.append(data)

        total_related_keywords = len(related_data)
        #print(related_data)
        #Fetch SERP data
        """try:
            start_position = request.args.get('start', default='1')
            num_results = request.args.get('num', 'us', default='1')

            start_position = int(start_position) - 1  # Adjust to 0-based index
            results = Google.search(keyword, num=num_results)

            end_position = min(start_position + int(num_results), len(results))

            formatted_results = []
            #print(formatted_results)
            for i in range(start_position, end_position):
                result = results[i]
                parsed_url = urlparse(result.link)
                domain = parsed_url.netloc
                formatted_results.append({
                    'position': i + 1,
                    'link': result.link,
                    'title': result.title,
                    'domain': domain
                })

            serp_analysis = {
                "Date": datetime.today().strftime('%B %d, %Y'),
                "Number of SERP Results": len(results),
                "SERP Results (Top 100)": formatted_results
            }

        except Exception as e:
            serp_analysis = {'error': str(e)}"""

        # Fetch RapidAPI data
        try:
            rapidapi_url = f"https://targeted-keyword-trend.p.rapidapi.com/{keyword}"  # Replace {keyword} with the actual keyword parameter
            headers = {
                "X-RapidAPI-Key": RAPIDAPI_KEY,
                "X-RapidAPI-Host": "targeted-keyword-trend.p.rapidapi.com"
            }
            response = requests.get(rapidapi_url, headers=headers)
            data = response.json()
            # Extract 'Month_Date_Year' and 'Search_Count' from each dictionary
            formatted_data = []
            
            for item in data:
               # print (data)
                if isinstance(item, dict):
                   month_date_year = item.get('Month_Date_Year')
                   search_count = item.get('Search_Count')
                   
                   formatted_data.append({'Month_Date_Year': month_date_year, 'Search_Count': search_count})

            form_data = []
            for entry in formatted_data:
               # Check if Month_Date_Year is None
                if month_date_year is None:
                    #return jsonify({"error": "Month_Date_Year is missing in the data."}), 400
                     form_data.append({"error": "Month_Date_Year is missing in the data."})
                else :
                    form_data.append({'Month_Date_Year': entry["Month_Date_Year"], 'Search_Count': entry["Search_Count"]})
        except Exception as e:
            form_data = {'error': str(e)}
            
        response = []

        if exact_match_data:
            response.append({"Keyword Overview": exact_match_data})
        else:
            response.append({"Keyword Overview": "No data found in the database for Keyword Overview."})

        # Filter and append only closely related keywords
        filtered_related_data = []
        for keyword_data in related_data:
            
            keyword = keyword_data[0].strip().lower()
            
            similarity_ratio = difflib.SequenceMatcher(None, sanitized_keyword, keyword).ratio()
            #if similarity_ratio >= 0.8 and len(keyword) > 2:  # Exclude very short words
            filtered_related_data.append({
                    "Keyword": keyword_data[0],  # Replace with appropriate column index
                    "Avg. monthly searches": keyword_data[1],  # Replace with appropriate column index
                    "Competition": keyword_data[2],  # Replace with appropriate column index
                    "Competition (indexed value)": keyword_data[3],  # Replace with appropriate column index
                    "Top of page bid (low range)": keyword_data[4],  # Replace with appropriate column index
                    "Top of page bid (high range)": keyword_data[5],  # Replace with appropriate column index
                    "Searches: Sep 2022": keyword_data[6],  # Replace with appropriate column index
                    "Searches: Oct 2022": keyword_data[7],  # Replace with appropriate column index
                    "Searches: Nov 2022": keyword_data[8],  # Replace with appropriate column index
                    "Searches: Dec 2022": keyword_data[9],  # Replace with appropriate column index
                    "Searches: Jan 2023": keyword_data[10],  # Replace with appropriate column index
                    "Searches: Feb 2023": keyword_data[11],  # Replace with appropriate column index
                    "Searches: Mar 2023": keyword_data[12],  # Replace with appropriate column index
                    "Searches: Apr 2023": keyword_data[13],  # Replace with appropriate column index
                    "Searches: May 2023": keyword_data[14],  # Replace with appropriate column index
                    "Searches: Jun 2023": keyword_data[15],  # Replace with appropriate column index
                    "Searches: Jul 2023": keyword_data[16],  # Replace with appropriate column index
                    "Searches: Aug 2023": keyword_data[17],  # Replace with appropriate column index
                    "Updated": formatted_date  # Use the calculated date from one week ago
                })
            #print (filtered_related_data)
        if filtered_related_data:
            filtered_related_data.insert(0, {"Total Related Keywords": len(filtered_related_data)})
            response.append({"Related Keywords": filtered_related_data})
            response.append({"Your Targeted Keword Trending History On Google": form_data })  # Add RapidAPI data to the response
            response.append({"SERP Analysis": serp_analysis})
            # Append Google Trends data to the response
            response.append({"Google Trends Data": google_trends_data})
            response.append({"Interest Google Trends Data": interest_data})
            fulldata = "Keyword Overview full analysis with SERP Analysis, Keword Trending History, related Queries and keyword intrest by region On Google Trends, Related Keywords data."
            response.append({"Comprehensive Keyword Analysis": fulldata })

        else :
            response.append({"Related Keywords": "No closely related keywords found in the database."})
            response.append({"Your Targeted Keword Trending History On Google": "No closely Your Targeted Keword Trending History On Google" })  # Add RapidAPI data to the response
            response.append({"SERP Analysis": "Sorry serp_analysis is not found. Please try after some time"})
   
        ordered_response = [
             response_item
            for name in ["Comprehensive Keyword Analysis", "Keyword Overview", "SERP Analysis", "Your Targeted Keword Trending History On Google", "Interest Google Trends Data", "Google Trends Data", "Total Related Keywords", "Related Keywords"]
           if (response_item := next((item for item in response if name in item), None))
           
     ]
        # Close the database connection
        cursor.close()

        # Return the response
        return jsonify(ordered_response)

    except Exception as e:
        datae = {"message": "Sorry.. No data found in my Database. if you try after 1 min, get big Keyword Research Data... or try another country"}
        return jsonify({'error': str(datae)}), 200
    
   # Route to fetch keyword data and SERP data from the MySQL database
@app.route('/google_keyword_ideas', methods=['GET'])
def google_keyword_Data():
    try:
        keyword = request.args.get('keyword')
        country = request.args.get('country')

        # Check if keyword and country are missing or invalid
        if not keyword or not re.match(r'^[a-zA-Z\s]+$', keyword) or not country:
            return jsonify({"error": "Invalid keyword or country parameter."}), 400
        if not keyword:
            return jsonify({"error": "Keyword parameter is missing."}), 400

        sanitized_keyword = keyword.strip().lower()

        # Define a list of allowed country codes
        allowed_countries = ['us', 'uk', 'ca', 'in']  # Add more as needed

        # Check if the provided country code is in the list of allowed countries
        if country.lower() not in allowed_countries:
            return jsonify({"error": "Invalid country code."}), 400
        
        sanitized_country = country.strip().lower()

         # Create a MySQL connection
        db_connection = mysql.connection
        cursor = db_connection.cursor()
    
        # Construct the table name based on the selected country
        table_name = f'google_keyword_data_{sanitized_country.lower()}'

        # Use string formatting to insert the table name into the SQL query
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        keywords_data = cursor.fetchall()
        if not table_name:
            # Handle the case where no data is found in the table
            return jsonify({"message": "No data found in the database."})

        response = []

         # Filter and append only closely related keywords
        filtered_related_data = []
        for keyword_data in keywords_data:
            keyword = keyword_data[0].strip().lower()
            similarity_ratio = difflib.SequenceMatcher(None, sanitized_keyword, keyword).ratio()
            if similarity_ratio >= 0.8:  # Exclude very short words
                filtered_related_data.append({
                    "Keyword": keyword_data[0],  # Replace with appropriate column index
                    "Avg. monthly searches": keyword_data[1],  # Replace with appropriate column index
                    "Competition": keyword_data[2],  # Replace with appropriate column index
                    "Competition (indexed value)": keyword_data[3],  # Replace with appropriate column index
                    "Top of page bid (low range)": keyword_data[4],  # Replace with appropriate column index
                    "Top of page bid (high range)": keyword_data[5],  # Replace with appropriate column index
                    "Searches: Sep 2022": keyword_data[6],  # Replace with appropriate column index
                    "Searches: Oct 2022": keyword_data[7],  # Replace with appropriate column index
                    "Searches: Nov 2022": keyword_data[8],  # Replace with appropriate column index
                    "Searches: Dec 2022": keyword_data[9],  # Replace with appropriate column index
                    "Searches: Jan 2023": keyword_data[10],  # Replace with appropriate column index
                    "Searches: Feb 2023": keyword_data[11],  # Replace with appropriate column index
                    "Searches: Mar 2023": keyword_data[12],  # Replace with appropriate column index
                    "Searches: Apr 2023": keyword_data[13],  # Replace with appropriate column index
                    "Searches: May 2023": keyword_data[14],  # Replace with appropriate column index
                    "Searches: Jun 2023": keyword_data[15],  # Replace with appropriate column index
                    "Searches: Jul 2023": keyword_data[16],  # Replace with appropriate column index
                    "Searches: Aug 2023": keyword_data[17],  # Replace with appropriate column index
                    "Updated": formatted_date  # Use the calculated date from one week ago
               })

        if filtered_related_data:
            filtered_related_data.insert(0, {"Total Related Keywords": len(filtered_related_data)})
            response.append({"Related Keywords": filtered_related_data})
        else:
            response.append({"Related Keywords": "No closely related keywords found in the database."})

        
        ordered_response = [
            response_item
            for name in ["Total Related Keywords", "Related Keywords"]
            if (response_item := next((item for item in response if name in item), None))
        ]

        # Close the database connection
        cursor.close()

        # Return the response
        return jsonify(ordered_response)

    except Exception as e:
        datae = {"message": "Sorry.. No data found in my Database. if you try after 1 min, get big Keyword Research Data... or try another country"}
        return jsonify({'error': str(datae)}), 200

if __name__ == '__main__':
    app.run(debug=True)
