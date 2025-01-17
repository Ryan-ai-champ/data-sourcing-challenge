# Under Lock and Key

## Introduction

You will create a `.env` file to store the API keys you created prior to this class, and test one of them out to ensure it works. You will then be able to copy your `.env` file into each activity folder that needs it.

## Instructions

### Store API keys

1. Navigate to the following resources to retrieve your API keys:

    * [NASA API](https://api.nasa.gov/) 

    * [OpenWeather](https://home.openweathermap.org/api_keys)

2. Create a new `.env` file, and declare the following environment variables:

    * `NASA_API_KEY` that stores your NASA API key.

    * `WEATHER_API_KEY` that stores your OpenWeather API key.

3. Open the Jupyter Notebook starter file, and import the Python `requests`, `os` and `dotenv` libraries.

### Execute API call with API key/env variable

4. Use the `load_dotenv()` method from the `dotenv` package to load and export the environment variables.

5. Use the `os.getenv()` function to retrieve the environment variable named `NASA_API_KEY`, and store it as a Python variable named `api_key`.

6. Use the `type` function to confirm the retrieval of the API key. Hint: If `NoneType` is returned, the environment variable does not exist. Revisit steps 2 and 3.

7. Review the [documentation](https://api.nasa.gov/) to create your`request_url`. Create a variable for the data and coordinates you want to search.

8. Concatenate the `request_url`, dates and coordinates, and `api_key` variables.  

9. Execute a `GET` request using Python requests library and the newly created `request_url`.

10. Display content to screen using the `json.dumps()`.

### Bonus: extract activityID from dict

If time permits, create a function called extract_activityID_from_dict that takes a dict as input such as in linkedEvents and extracts the value of that dict .


---

© 2023 edX Boot Camps LLC. Confidential and Proprietary. All Rights Reserved.
