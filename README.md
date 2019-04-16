## Live Twitter Data Streamer, Filterer, and Processor
#### Alex John Quijano

**Description.** Twitter is a data mine rich of highly complex dataset which can show - but not limited to - word frequencies, user-user interactions, topic discourse, and even culture specific language (i.e. emojis, word abreviations).

**Purpose.** The scripts on this repository provides an easy way to stream, filter, and process live tweets iteratively for researchers interested in data science, mathematical modeling, computational linguistics, historical linguistics, and/or discourse analysis.

**Warning.** Streaming live tweets for long periods is not recommended because the filesize gets very large unless you have reasonable file storage. The streamer also can capture any public tweets (protected tweets are not captured). That means it can capture, tweets from bot users, pornography, and profanity; when viewing this tweets, it might be NSFW.

**References.**

1. Bird, Steven, Ewan Klein, and Edward Loper. Natural language processing with Python: analyzing text with the natural language toolkit. " O'Reilly Media, Inc.", 2009.

**License.** See LICENSE.md file.

**Required Programs.**
1. python 3.6.5
2. GNU bash 4.3.48
3. jupyter 4.4.0

**Python Modules.**
1. subprocess
2. numpy
2. nltk (https://www.nltk.org/)

**Character Encoding.**
1. UTF-8

**Twitter Application User Interface (API).** You must have keys and tokens of your Twitter API to use the streamer (see INSTRUCTIONS).

**Instructions - Website.** https://stressosaurus.github.io/raw-data-twitter-tweets/

**Getting Started - Jupyter Notebook.** Clone this repository and run the following command on the terminal with the correct working directory.

``
jupyter notebook INSTRUCTIONS.ipynb
``
