#!/usr/bin/env python
# coding: utf-8

# Author: Raghu Raman Nanduri
# 
# Date: Apr 28 2019
#     
# DSC 550, Week7

# In[184]:


import pandas as pd
import numpy as np


# In[185]:


# Read CSV files into pandas dataframe


movies_df = pd.read_csv('movies.csv')

ratings_df = pd.read_csv('ratings.csv')


# In[186]:


movies_df.head()


# In[187]:


ratings_df.head()


# In[188]:


# Merging data sets to create matrix of user ratings for each movie


# In[189]:


user_movie_ratings_df = pd.merge(movies_df, ratings_df, on='movieId')
user_movie_ratings_df.head()


# # Exploratory Data Analysis

# In[190]:


# Let us create a dataframe with average rating for each movie and number of ratings
ratings = pd.DataFrame(user_movie_ratings_df.groupby('title')['rating'].mean())
ratings.head()


# In[191]:


# Adding number of ratings as column
ratings['number_of_ratings'] = user_movie_ratings_df.groupby('title')['rating'].count()
ratings = ratings.sort_values("number_of_ratings", ascending = False)
ratings['number_of_ratings'] = ratings['number_of_ratings'].astype(int)
ratings.head()


# In[192]:


top200_mostrated_df = ratings.query('number_of_ratings > 200')
top200_mostrated_df.head()


# In[193]:


# Average rating across all movies
All_Avg_Rating = ratings['rating'].mean()
All_Avg_Rating


# In[194]:


ratings['weighted_avg_rating'] = (ratings['rating'] * ratings['number_of_ratings']).sum()/ratings['number_of_ratings'].sum()

ratings.head()


# In[ ]:





# In[195]:


# Creating matrix based on merged df above
movie_matrix_df = user_movie_ratings_df.pivot_table(index='userId', columns='title', values='rating')
movie_matrix_df.head()


# In[196]:


# Creating matrix based on merged df above - Using movieid, instead of title
movie_matrix_df2 = user_movie_ratings_df.pivot_table(index='userId', columns='movieId', values='rating')
movie_matrix_df2.head()


# In[197]:


# Filling "NAN" with "0" for computation
movie_matrix_df = movie_matrix_df.fillna(0)
movie_matrix_df.head()


# In[ ]:





# In[198]:


movie_matrix_df.describe()


# In[199]:


# Converting movie_matrix_df into matrix

movie_matrix = movie_matrix_df.as_matrix()
np.shape(movie_matrix)


# In[200]:


# Checking a subset of matrix
movie_matrix[0:5, 0:5]


# In[201]:


# Measuring Movie similarity using cosine for Item based filtering


# In[202]:


from sklearn.metrics.pairwise import pairwise_distances 
from sklearn.metrics.pairwise import cosine_similarity


# In[203]:


movie_similarity = pairwise_distances(movie_matrix, metric='cosine')
movie_similarity


# In[204]:


user_similarity = cosine_similarity(movie_matrix_df) 
user_similarity


# In[205]:


np.shape(user_similarity)


# In[206]:


user_similarity_df = pd.DataFrame(user_similarity)
user_similarity_df.head()


# In[207]:


# Creating movie similarity matrix

movie_matrix2 = movie_matrix.T
np.shape(movie_matrix2)


# In[208]:


#movie_matrix_df.T


# In[209]:


#movie_matrix_df.T['Lethal Weapon (1987)']

movie_matrix_df2 = movie_matrix_df.T
#movie_matrix_df2


# In[210]:


movie_matrix_df2.loc['Lethal Weapon (1987)']


# In[ ]:





# In[211]:


movie_matrix2


# In[212]:


movie_similarity = cosine_similarity(movie_matrix_df2) #(movie_matrix2) 
np.shape(movie_similarity)


# In[213]:


movie_similarity_df = pd.DataFrame(movie_similarity)
movie_similarity_df.head()


# In[ ]:





# In[214]:


# Defining test data

test_ratings = [
    ('Lethal Weapon (1987)',4),
    ('Natural Born Killers (1994)',3),
    ('Inception (2010)',0),
    ('Heat (1995)',0),
    ('Finding Nemo (2003)',3),
    ('Office Space (1999)',1),
    ('Home Alone (1990)',0),
    ('High Fidelity (2000)',2),
    ('Donnie Darko (2001)',3),
    ('Lion King, The (1994)',2)
]


# In[215]:


# COnverting list of tuples into dataframe
test_ratings_df = pd.DataFrame(test_ratings, columns = ['title', 'rating'])
print(type(test_ratings_df))


# In[216]:


test_ratings_df['userid'] = 'R007'
test_ratings_df


# In[217]:


test_ratings_piv_df =  test_ratings_df.pivot_table(index='userid', columns='title', values='rating')
test_ratings_piv_df


# In[218]:


test_ratings_matrix = test_ratings_df.as_matrix()


# In[ ]:





# In[219]:


for row in test_ratings_df:
    print(row)


# In[220]:


for row in movie_matrix_df:
    #print(row)
    similarity = (test_ratings, row)
    print(similarity)
       # other_user_suggested_ratings = []


# In[221]:


user_input_movies = []
user_input_movies_sim = [] # 

for movie, rating in test_ratings:
        user_input_movies.append(movie)
        #recommends similar movies for movies the user gave 3 or higher to 
        if rating >=3:
            user_input_movies_sim.append(movie)
            


# In[222]:


user_input_movies_sim


# In[223]:


user_input_movies


# In[224]:


# Get index of this movie from its title

def column_index(df, query_cols):
    cols = df.columns.values
    sidx = np.argsort(cols)
    return sidx[np.searchsorted(cols,query_cols,sorter=sidx)]


movie_index = column_index(movie_matrix_df, 'Lethal Weapon (1987)')
movie_index


# In[225]:


# Get moviename from the index of the columns

def titlename_index(df, query_cols):
    cols = df.columns.values
    sidx = np.argsort(cols)
    return cols[query_cols]


movie_name = titlename_index(movie_matrix_df, 4642)
movie_name


# In[226]:


test_rating_similarity = list(enumerate(movie_similarity_df[movie_index]))
#test_rating_similarity = movie_similarity_df[movie_index]
test_rating_similarity


# In[227]:


movie_index = []
user_input_similarity = []
user_input_similarity_scores = []

for movie in user_input_movies_sim: #user_input_movies:
    #print(movie)
    get_user_inputmovie_index = column_index(movie_matrix_df, movie)
    movie_index.append(get_user_inputmovie_index)
    test_rating_similarity = list(enumerate(movie_similarity_df[get_user_inputmovie_index]))
    #print(test_rating_similarity)
    #user_input_similarity.append(test_rating_similarity)
    # Sort the movies based on the similarity scores
    test_rating_similarity_scores = sorted(test_rating_similarity, key=lambda x: x[1], reverse=True)
    # Select top 10 movies for each movie entered by user; row 0 is same movie, so select from 1 till 11
    test_rating_similarity_scores = test_rating_similarity_scores[1:11]    
    #print(test_rating_similarity_scores)
    user_input_similarity_scores.append(test_rating_similarity_scores)
    print("Because you liked {} you may like the following movies as well: ". format(movie))
    for i in range(len(test_rating_similarity_scores)):
        print("  ", titlename_index(movie_matrix_df, test_rating_similarity_scores[i][0]))


# In[228]:


def movie_recommender(user_input, all_user_ratings):
    
    user_input_movies = []
    user_input_movies_sim = [] # 
    recommended_movie_list = []
    base_movie = []
    similar_movie = []
    zipped_data_list = []

    for movie, rating in user_input:
            user_input_movies.append(movie)
            #recommends similar movies for movies the user gave 3 or higher to 
            if int(rating) >=3:
                user_input_movies_sim.append(movie)
                
    movie_index = []
    user_input_similarity = []
    user_input_similarity_scores = []
    zipped_data = []

    for movie in user_input_movies_sim: #user_input_movies:
        
        base_movie = []
        similar_movie = []
        
        get_user_inputmovie_index = column_index(movie_matrix_df, movie)
        movie_index.append(get_user_inputmovie_index)
        test_rating_similarity = list(enumerate(movie_similarity_df[get_user_inputmovie_index]))
        
        # sorting on similarity score for the movie user liked (rating > 3)
        test_rating_similarity_scores = sorted(test_rating_similarity, key=lambda x: x[1], reverse=True)
        
        # Select top 10 movies for each movie entered by user; row 0 is same movie, so select from 1 till 11
        test_rating_similarity_scores = test_rating_similarity_scores[1:11]    
        
        user_input_similarity_scores.append(test_rating_similarity_scores)

        print("As you liked {} you may like the following movies as well: ". format(movie))
        for i in range(len(test_rating_similarity_scores)):
            print("  ", titlename_index(movie_matrix_df, test_rating_similarity_scores[i][0]))
        
    
    
    return 
     


# In[229]:


movie_recommender(test_ratings, movie_matrix)


# In[ ]:





# In[230]:


def movie_recommender2(user_input, all_user_ratings):
    
    user_input_movies = []
    user_input_movies_sim = [] # 
    recommended_movie_list = []
    base_movie = []
    similar_movie = []
    zipped_data_list = []
    movie_score_list = []
    score_list = []

    for movie, rating in user_input:
            user_input_movies.append(movie)
            #recommends similar movies for movies the user gave 3 or higher to 
            if int(rating) >=3:
                user_input_movies_sim.append(movie)
                
    movie_index = []
    user_input_similarity = []
    user_input_similarity_scores = []
    zipped_data = []

    for movie in user_input_movies_sim: #user_input_movies:
        
        base_movie = []
        similar_movie = []
        
        get_user_inputmovie_index = column_index(movie_matrix_df, movie)
        movie_index.append(get_user_inputmovie_index)
        test_rating_similarity = list(enumerate(movie_similarity_df[get_user_inputmovie_index]))
        
        # sorting on similarity score for the movie user liked (rating > 3)
        test_rating_similarity_scores = sorted(test_rating_similarity, key=lambda x: x[1], reverse=True)
        
        # Select top 10 movies for each movie entered by user; row 0 is same movie, so select from 1 till 11
        test_rating_similarity_scores = test_rating_similarity_scores[1:11]    
        
        user_input_similarity_scores.append(test_rating_similarity_scores)

        #print("As you liked {} you may like the following movies as well: ". format(movie))
        for i in range(len(test_rating_similarity_scores)):
            #print("  ", titlename_index(movie_matrix_df, test_rating_similarity_scores[i][0]))
            movie_score_list.append(titlename_index(movie_matrix_df, test_rating_similarity_scores[i][0]))
            score_list.append(test_rating_similarity_scores[i][1])
        
    
    
    recmovlist = dict(zip(movie_score_list, score_list))
    
    # Convert returned list into data frame
    recmovlist_df = pd.DataFrame(recmovlist.items(), columns = ['recommended_movie', 'similarity_score'])

    # Sort the dataframe based on similarity score
    recmovlist_df = recmovlist_df.sort_values("similarity_score", ascending = False)

    recmovlist_df = recmovlist_df.reset_index(drop = True)


    # Return only 10 movies after excluding the movies that user rated

    movie_list = []
    score_list = []
    final_recomd_movie_df = pd.DataFrame()

    for index, row in recmovlist_df.iterrows():
        movie = row[0]
        score = row[1]

        # discarding the movie from recommendation list if the movie was rated already
        if movie not in test_ratings_df['title']:
            movie_list.append(row[0])
            score_list.append(row[1])


    final_recomd_movie_df['movie'] = pd.Series(movie_list)
    final_recomd_movie_df['score'] = pd.Series(score_list)



    
    return final_recomd_movie_df
     


# In[231]:


movie_recommender2(test_ratings, movie_matrix)


# In[ ]:





# # 2. Movie Search Engine

# In[ ]:





# In[232]:


from nltk.util import ngrams
from fuzzywuzzy import fuzz
import re


# In[233]:


samplengramlist = list(ngrams("batman", 2))


# In[234]:


sample_ngrams = [''.join(ngram) for ngram in samplengramlist]
sample_ngrams


# In[235]:


def create_ngram(string):
    
    string = string.lower()
    string = re.sub(r' ', '', string)
    ngramlist = list(ngrams(string, 2))
    ngramlist2 = [''.join(ngram) for ngram in ngramlist]
    return ngramlist2


# In[236]:


samplengrams = create_ngram("temp  Tation")
samplengrams


# In[237]:


movies_df.head()


# In[238]:


movies_list = movies_df['title'].unique()
movies_list


# In[239]:


def give_matching_list2(user_input, matching_percentage):
    
    fuzz_score_tup = []
    search_gram = create_ngram(user_input)
    for i in range(len(movies_list)):
        movie_ngram = create_ngram(movies_list[i])
        fuzz_score = fuzz.partial_ratio(search_gram,movie_ngram )
        if fuzz_score > matching_percentage:
            #fuzz_score_tup.append([user_input, search_gram, movies_list[i], movie_ngram, fuzz_score])
            fuzz_score_tup.append([user_input, movies_list[i], fuzz_score])
    return fuzz_score_tup


def movie_search2(user_input, percentage):
    
    movie_list = []
    matching_list = give_matching_list(user_input, percentage)
    for i in range(len(matching_list)):
        #print(matching_list[i][1])
        movie_list.append(matching_list[i][1])
    return movie_list
    


# In[240]:


def give_matching_list(user_input):
    
    fuzz_score_tup = []
    search_gram = create_ngram(user_input)
    for i in range(len(movies_list)):
        movie_ngram = create_ngram(movies_list[i])
        fuzz_score = fuzz.partial_ratio(search_gram,movie_ngram )

        fuzz_score_tup.append([user_input, movies_list[i], fuzz_score])
        
    #return sorted(fuzz_score_tup, key=lambda x: x[1], reverse=True)
    return fuzz_score_tup #.sort(key = lambda r:r[2], reverse=True)


# In[241]:


matching_movie_list = give_matching_list("Temp tation")
matching_movie_list


# In[242]:


def movie_search(user_input):
    
    movie_list = []
    matching_list = give_matching_list(user_input)
    
    matching_list_df = pd.DataFrame(matching_list, columns = ['Searched_movie', 'Matchin_movie', 'Matching_score'])
    
    matching_list_df = matching_list_df.sort_values('Matching_score', ascending=False).head(5)#.tolist()
    
    movie_list = matching_list_df.Matchin_movie.head(5).tolist()
    return movie_list
    


# In[243]:


searched_list = movie_search("A venGers")
searched_list


# In[ ]:





# In[ ]:





# # 3. Movie Recommendation Application

# In[ ]:





# In[244]:


find_movie = input('Enter movie ')


# In[245]:


print(find_movie)


# In[246]:


def get_rating():
    
    find_movie = input('Enter movie : ')
    
    movie_options = movie_search(find_movie)
    
    for i in range(len(movie_options)):
        j = i+1
        print(str(j) + " - ",movie_options[i] )
    
    print(str(6) + " - ", "I don't see what I am looking for")
    
    Entered_choice = input('\n Select a movie:')
    
    if Entered_choice == 6 :
        print("\n Try again \ n")
        
        
    selected_movie = movie_options[int(Entered_choice)-1]
    

    print("\n selected movie is : ", selected_movie )
    print('\n How much did you enjoy movie on a scale of 0 to 5:')
    Entered_move_rating = input('(0 - hated it, 5 - loved it) : ')
   
    #if Entered_move_rating not in ['0', '1', '2', '3', '4', '5']:
        #print("\n Select option between 0 to 5 ")
        #Entered_move_rating = input('(0 - hated it, 5 - loved it) : ')
    #print(Entered_move_rating)
    
    #movie_rating_list = []
    movie_rating_list = (selected_movie, Entered_move_rating)
    
    return movie_rating_list
    


# In[247]:


get_rating()


# In[248]:


# Counter to restrict the movie search to 5 times
min_ratings_for_recomm = 5

def recomm_app():
    '''
    Function that lets user search for movie and rate upto 5 movies and returns
    recommended list of 10 movies for each movie the user liked (rating >3)
    '''
    
    full_rating_list = []
    
    for i in range(min_ratings_for_recomm):
        #print(i)
        rating_list = get_rating()
        full_rating_list.append(rating_list)
        
        # Below logic ensures to ask for movie selection only 5 times
       
        if i < 4:
            print("\n To get recommendations need to rate atleast " + str(min_ratings_for_recomm- (i+1)) 
                  + " more movies \n ")
            catch_input = input(' To exit enter n \n To continue enter y:')
            if catch_input == 'n':
                break
        
        if i == 4 :
            print("Selected movies, ratings are: \n")
            print(full_rating_list)
            print("\n Selected all 5 movies. Now lets look at the recommendations \n")
        
    return movie_recommender(full_rating_list, movie_matrix)
        


# In[249]:


# Run the app
recomm_app()


# In[250]:


# Counter to restrict the movie search to 5 times
min_ratings_for_recomm = 5

def recomm_app2():
    '''
    Function that lets user search for movie and rate upto 5 movies and returns
    recommended list of movies that have similarity score with respective to the 
    movies users liked (rating >3)
    '''
    
    full_rating_list = []
    
    for i in range(min_ratings_for_recomm):
        #print(i)
        rating_list = get_rating()
        full_rating_list.append(rating_list)
        
        # Below logic ensures to ask for movie selection only 5 times
       
        if i < 4:
            print("\n To get recommendations need to rate atleast " + str(min_ratings_for_recomm- (i+1)) 
                  + " more movies \n ")
            catch_input = input(' To exit enter n \n To continue enter y:')
            if catch_input == 'n':
                break
        
        if i == 4 :
            print("Selected movies, ratings are: \n")
            print(full_rating_list)
            print("\n Selected all 5 movies. Now lets look at the recommendations \n")
        
    return movie_recommender2(full_rating_list, movie_matrix)
        


# In[251]:


# Run the app
recomm_app2()


# In[ ]:





# In[ ]:




