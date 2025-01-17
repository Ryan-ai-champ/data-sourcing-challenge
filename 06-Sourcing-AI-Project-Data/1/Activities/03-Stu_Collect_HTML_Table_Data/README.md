# Collect HTML Table Data

## Introduction

In this activity, you will use `read_html` from Pandas to scrape a Wikipedia article. You will then clean the resulting DataFrame and export it to a CSV.

## Instructions

1. Use the Pandas `read_html` method to parse the URL.

2. There are many tables on this Wikipedia page. Locate the "Overall Results By Team" table towards the end of the page and convert it to a DataFrame.

    ![A screenshot of the first seven rows of the "Overall Results By Team" table from Wikipedia](https://static.bc-edx.com/ai/ail-v-1-0/m6/lesson_1/img/overall-results-by-team-table.png)

3. Drop the final row for the "Total" from the DataFrame.

4. Check the data types of the columns.

5. The "GD" and "AGD" columns require some cleaning before they can be converted to numeric columns. Note that the longer hyphen (`−`) from the original table is subtly different from the minus sign (`-`) that can be converted to a numeric data type. The following changes should be made:

    * Remove the plus sign (`+`).

    * Replace the longer hyphen (`−`) with a minus sign (`-`).

6. Use a loop to convert the following columns to data type `int`: "Pld", "D", "GD". Then convert the "AGD" column to data type `float`.

7. Export the resulting DataFrame to a CSV file without the index.

---

© 2023 edX Boot Camps LLC. Confidential and Proprietary. All Rights Reserved.
