'''Simplest MapReduce Program in Python using 
the package MRJob
Problem: Output the movie thats rated most
Data: ml-100k\u.data from http://grouplens.org/
Author: anoop
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRatedMovie(MRJob):
    
    def configure_options(self):
        super(MostRatedMovie, self).configure_options()
        self.add_file_option('--items',help='Path to u.item')
    
    def steps(self):
        return [
        MRStep(mapper=self.mapper,
               reducer_init = self.reducer_init,
               reducer=self.reducer_count_ratings),
        MRStep(reducer=self.reducer_find_max)
    ]
    
    def mapper(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
    
    def reducer_init(self):
        self.movieNames = {}
        with open("u.ITEM") as fh:
            for line in fh:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]
        
    def reducer_count_ratings(self, movieID, occurences):
        yield None, (sum(occurences), self.movieNames[movieID])
        
    def reducer_find_max(self, key, myTuples):
        yield max(myTuples) 

if __name__ == '__main__':
    MostRatedMovie.run()
