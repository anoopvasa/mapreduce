'''
Problem: 
    Calculate word frequency of a book

Data:
    Any book from PROJECT GUTENBERG
     
Author:anoop
'''
from mrjob.job import MRJob

class WordFrequency(MRJob):
    def mapper(self, key, line):
        words = line.split()
        for word in words:
            yield word, 1
    
    def reducer(self, word, count):
        yield word, sum(count)


if __name__ == '__main__':
    WordFrequency.run()
