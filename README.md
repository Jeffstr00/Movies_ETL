# Movies ETL - Extract, Transform, Load

## Overview

At Amazing Prime Video (the streaming site for the world's largest retailer), they have decided that they haven't yet made enough money, and they need to make even more!  One good way to do that is to pre-emptively predict which low-budget movies will become popular so that they can take advantage and scoop them up on the cheap, maximizing profits.  In order to make those predictions, the company is hosting a hack-a-thon where teams are tasked with predicting popular pictures.  In order to do this, they need a clean set of data to work with, which is where we come in.

We were given huge amounts of data from both Wikipedia and MovieLens, but this data is not always clean, uniform, complete, or even accurate.  It was our job to extract the data, clean it (as best as possible), and combine it into one relatively easy to read location.  After completing that task, the Amazing Prime team decided that they liked the work so much that they want it so that it can be updated on a regular basis.  As a result, we were then tasked with taking our original ETL code, refactoring it into a formula (instead of a one-time use code), and ultimately loading our cleaned and updated data into a PostgreSQL database.

## Results

### Cleaning Movies

The data we were provided with had thousands of rows full of data (spanning 20+ columns).  Unfortunately, the massive dataset (especially from Wikipedia) was not always uniform, complete, or even accurate.  Fixing things manually was not practical, so we created and used many functions and other bits of code that would automate the process.  We made a clean_movie function which read in each movie, added its alternate title (if it had one) to one column, and then changed column names to be uniform using, for instance, `change_column_name('Distributed by', 'Distributor')`.

### ETL Function (with RegEx)

Our extract_transform_load function did the rest of the work.  Duplicate movies were caught and dropped using `wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)`.  RegEx values (for instance: `r'\$\s*\d+\.?\d*\s*[mb]illi?on'` for something like "$27.5 million") were used to search out data in different forms, so that it could then be stored in one uniform format.  The same was done for release dates, where we captured the date, no matter what form it was given as, by using four different RegEx variables.
        
### Combining Kaggle + Wiki Data

Luckily, the Kaggle data was much cleaner, but it still wasn't always complete.  After going through each column, we determined that, whenever possible, it was always best to go with the Kaggle data.  However, if that data was missing, we would then fill it in using data from Wikipedia.  We accomplished this by using `df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)`.

### Ratings DataFrame

To create the ratings DataFrame, we used the .groupby function to grab ratings associated with each movieId.  We then renamed "userId" to "count" and then "pivoted" the table on movieId so that we would be left with a DataFrame showing us how many instances of each rating each movie received.
`rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating',values='count')`
                    
### Load to SQL Database

Finally, to send our cleaned DataFrames into an updated PostgreSQL database, we first set up a connection string: `db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5432/movie_data" and database engine: `engine = create_engine(db_string)`.  We were then able to save it to the SQL table using `movies_df.to_sql(name='movie', con=engine, if_exists='replace')`.  We did the same for the ratings file, though we broke that into sections due to how large it was.

![Movies Query](https://github.com/Jeffstr00/Movies_ETL/blob/main/Resources/movies_query.png)

![Ratings Query](https://github.com/Jeffstr00/Movies_ETL/blob/main/Resources/ratings_query.png)
