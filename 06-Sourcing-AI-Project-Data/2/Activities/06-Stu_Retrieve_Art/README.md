# Retrieving Articles

## Introduction

In this activity, you will create an application that grabs art from the the Metropolitan Museum of Art Collection API (Met API), stores them within a list, and prints snippets of the art details to the screen.

## Instructions

1. Review the [https://metmuseum.github.io/](https://metmuseum.github.io/) to assist you in building your query URL.

2. Search for art that contains "Cézanne"

3. Limit your search to art within a time period of 1850 to 1999

4. Search for paintings only.

5. Limit your search to art that is currently on view at the Met.

6. Limit your search to art that has a corresponding image in the API.

7. Build your query URL, and save it to a variable.

8. Retrieve a response from the Met API with a get request.

9. Traverse through the returned JSON to retrieve the list of `ObjectIDs` and store it in a variable.

10. Loop over the `ObjectIDs` and filter our the pieces of art where the artist is Cézanne.

11. Use a sleep fuction to stay within the query limits of the Met API.


---

© 2023 edX Boot Camps LLC. Confidential and Proprietary. All Rights Reserved.
