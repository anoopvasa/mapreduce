'''
Problem: Collaborative Filtering on Movie Ratings
that indirectly support Movie Recommendations.

Data: Grouplens.org

To execute: python MovieRecommendations.py -r emr --num-ec2-instances 8 --item
s=ml-100k/u.item ml-100k/u.data > sims1.txt

Author: anoop
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

class MovieRecommendations(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_user_to_ratings,
                    reducer=self.reduce_user_by_ratings),
            MRStep(mapper=self.map_combinations,
                    reducer=self.reduce_ratings_to_score),
            MRStep(mapper=self.map_shuffle,
                    mapper_init=self.mapper_init,
                    reducer=self.reduce_shuffle)]
    
    def configure_options(self):
        super(MovieRecommendations, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')
    
    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)
        
    def map_user_to_ratings(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, (movieID, float(rating))
        
    def reduce_user_by_ratings(self, userid, movie_rating):
        mov_rating = []
        for movie, rating in movie_rating:
            mov_rating.append((movie,rating))
        yield userid, mov_rating
    
    def map_combinations(self, userid, itemRatings):
        
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            movieID1 = itemRating1[0]
            rating1 = itemRating1[1]
            movieID2 = itemRating2[0]
            rating2 = itemRating2[1]

            yield (movieID1, movieID2), (rating1, rating2)
            yield (movieID2, movieID1), (rating2, rating1)
            
    def mapper_init(self):
        self.movieDB = {}
        with open("u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieDB[int(fields[0])] = fields[1]
   
    def reduce_ratings_to_score(self, movie, rating):
        score, n = self.cosine_similarity(rating)
        if score > 0.95 and n > 10:
            yield movie, (score, n)
            
    def map_shuffle(self, movie, score):
        score, n = score
        movie1, movie2 = movie
        yield (self.movieDB[int(movie1)], score), (self.movieDB[int(movie2)], n)
        
    def reduce_shuffle(self, movie1, movie2):
        movie, score = movie1
        for movie2, n in movie2:
            yield movie1, ( movie2, score, n )
        
if __name__ == '__main__':
    MovieRecommendations.run()
        
        
              
        
        