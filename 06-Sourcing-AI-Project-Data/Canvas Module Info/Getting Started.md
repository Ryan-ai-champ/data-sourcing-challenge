---
title: "6: Getting Started"
---

<img style="display: none;"  src="https://static.bc-edx.com/ai/ail-v-1-0/m6/lms/img/banner.png" alt="lesson banner" />

For this module, you will install some new Python libraries in your `dev` environment. Don't forget to activate your `dev` environment prior to launching Jupyter Notebook.

Additionally, you will need to apply for API keys prior to the second class. Sometimes API keys can take some time to become active and be sent to you. Try to set up these keys 24 hours before your second class.

Finally, you will be working with environment variables in a `.env` file, which will store your API keys. These keys should remain secret and should not be uploaded to a public GitHub repository, where other people can find and use them. You are not required to pay anything to use the APIs in this course, but you might be in the real world, so revealing such a key in a public repository could lead to incurred charges not related to your own projects. At the end of this page, you will be instructed how to add a `.gitignore` file when creating your repository so that you don't accidentally push your `.env` file to GitHub.

### Install Required Libraries

In this module, you will use some new Python libraries that you should install in your `dev` virtual environment. To do so, follow these steps:

1. Open Anaconda Prompt (in Windows) or a Terminal window (in macOS).

2. Activate your virtual environment by typing `conda activate dev` and pressing Enter.

### Libraries Included with Anaconda

You’ll use the following Python modules and libraries to facilitate API requests:

* OS: The OS module comes under Python's standard utility models and provides functions for interacting with the computer's operating system. The OS module does not require a separate download.

* Requests: The Python Requests library helps you access data via APIs.

* JSON: This library puts the response (that is, the data) from an API into a human-readable format.

The Requests and JSON libraries get installed with Anaconda. To confirm their installation, do the following:

In the terminal, activate the Conda development environment, and then run the following code:

```shell
conda list requests

conda list json
```

The result on your screen should resemble the following text:

```text
(dev) % conda list requests
# packages in environment at /Library/anaconda3/envs/dev:
#
# Name                    Version                   Build  Channel
requests                  2.31.0                   pypi_0    pypi
requests-file             1.5.1              pyhd3eb1b0_0  
(dev) % conda list json    
# packages in environment at /Library/anaconda3/envs/dev:
#
# Name                    Version                   Build  Channel
json5                     0.9.6              pyhd3eb1b0_0  
jsonschema                4.17.3          py310hca03da5_0  
python-fastjsonschema     2.16.2          py310hca03da5_0  
python-lsp-jsonrpc        1.0.0              pyhd3eb1b0_0  
ujson                     5.4.0           py310hc377ac9_0  
```

If your development environment is missing either package, you can directly install it.

To install the Requests library, check that your development environment is active, and then run the following command:

```shell
conda install -c anaconda requests
```

To install the JSON library, check that your development environment is active, and then run the following command:

```shell
conda install -c jmcmurray json
```

#### Install the `python-dotenv` Library

With the `python-dotenv` library, you can read key-value pairs from an environment file (`.env`) and add them as environment variables.

To install this library, run the following command in your terminal:

```shell
pip install python-dotenv
```

#### Install the US Census Library

To install [census](https://github.com/datamade/census), a Python wrapper for the United States Census Bureau's API, run:

```shell
pip install census
```

After installing this library, restart Jupyter Notebook if it was running. You must launch Jupyter Notebook from your Python environment for your required libraries to work.

If you have any issues with the installation, we recommend that you attend office hours before the third class.

### Set Up API Keys

#### NASA

1. Navigate to the [NASA API site](https://api.nasa.gov/) and click "Generate API Key". Enter your name, email address, and (optionally) a statement that you will be using the API for educational purposes.

2. Navigate to the email used to sign up, and you should find an email there with your API key.

    > **Note:** You may have to check your spam folder for the email.

3. Retain this key somewhere securely because you will not be able to retrieve it from the NASA site in the future.

> **Important:** This API key will be used in this week's Challenge. The NASA API also has a rate limit of 30 requests per IP address per hour.

#### The Movie Database

1. Fill out the [sign up](https://www.themoviedb.org/signup) form on The Movie DB website.

2. Verify your account with the email they send, and log in.

3. To register for an API key, click the [API link](https://www.themoviedb.org/settings/api) from within your account settings page.

    For more information, visit the Developer [Getting Started](https://developer.themoviedb.org/docs/getting-started) page.

4. When prompted with the question "What type of API key do you wish to register?" click "Developer," as captured in the following image.

    ![A screenshot of the TMDB API key sign up with a choice of "Developer" and "Professional"](https://static.bc-edx.com/ai/ail-v-1-0/m6/lms/img/tmdb-api-key-type.png)

5. Read and accept the [terms of use](https://www.themoviedb.org/documentation/api/terms-of-use).

    > **Note:** This API is free for personal and educational uses, but if you want to use it to develop an app for commercial use, then there are specifics you should be aware of in the terms of use.

6. Fill out the form to set up your API key. You may use the following settings:

    ![A screenshot of the top of the TMDB API key form.](https://static.bc-edx.com/ai/ail-v-1-0/m6/lms/img/tmdb-fill-form.png)

    Type of Use: Education

    Application Name: learning-apis

    Application URL: N/A

    Application Summary: Testing out TMDB API to use as part of my learning APIs process.

    > **Note:** This form will return errors if you do not fill out all the required fields.

7. After submitting the form, you will be taken to the API Settings Overview page, where you will be able to copy your API key to use in class.

    ![A screenshot of the TMDB API Settings Overview page.](https://static.bc-edx.com/ai/ail-v-1-0/m6/lms/img/tmdb-api-key.png)

> **Important:** This API key will be used in this week's Challenge. TMDB also has a rate limit of about 50 requests per second.

#### OpenWeatherMap API

1. Sign up for an account at the OpenWeatherMap API [Sign Up](https://home.openweathermap.org/users/sign_up) page.

2. Once you are signed in, visit the [API Keys](https://home.openweathermap.org/api_keys) page to get or generate an API key for class.

#### US Census

1. Obtain a US Census API key from the US Census Bureau [sign up](https://api.census.gov/data/key_signup.html) page.

2. Check your email. Open the email from "Census Data API Key Request" and click the link to activate your key. You will use the API key provided in the email.

If you have any issues with setting up your API keys, we recommend that you attend office hours before the second class.

### Create a New GitHub Repository with a `.gitignore` File

> **Important:** You will need to follow these instructions so you don't accidentally reveal your API keys in a public repository, which you will need to do for this module's Challenge.

1. Log in to your GitHub account and select “New repository.”
2. Name the repository.
3. Make the repository public.
4. Check the box that says "Initialize the repository with a README."
5. Add a `.gitignore` file to the repository. GitHub does not track files or files with extensions that are added to the `.gitignore` file. Begin typing "Python," and then select Python as the option.

    ![Create a new repository and select Python as the type of .gitignore file.](https://static.bc-edx.com/ai/ail-v-1-0/m6/lms/img/gitignore-python-as-the-type.png)

6. Click the green "Create repository" box. You should see the `.gitignore` file in the repository. Later in this module, we'll edit the `.gitignore` file.

    ![The .gitignore file in the repository](https://static.bc-edx.com/ai/ail-v-1-0/m6/lms/img/gitignore-file-in-repository.png)

7. On the repository's main page, click the green button (top right) with the text "Code."
8. Select "Clone with HTTPS" and click the clipboard icon to copy the clone URL for the repository.
9. Open the command line (macOS) or Git Bash (Windows), and navigate to the directory that contains your GitHub repositories.
10. Once in the directory, type `git clone` followed by a space, and then paste the URL that you copied. Press Enter.

If you are using macOS or collaborating with others who use macOS, you will want to edit your `.gitignore` file to add the hidden `.DS_Store` folders, so they aren't added when you perform a `git add .` action.

1. Open VS Code and locate the directory of your repo, then open the `.gitignore` file.

2. Add the following lines to the file (either the start or end of the file will work):

    ```text
    # .DS_Store
    .DS_Store
    ```

3. Save and close the file.

4. Return to the command line (macOS) or Git Bash (Windows), and navigate to the directory of the GitHub repository you just cloned.

5. Add the `.gitignore` file and push your changes to the repository.
