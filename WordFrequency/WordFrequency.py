'''
Problem: 
    Calculate word frequency of a book

Data:
    Any book from PROJECT GUTENBERG
     
Author:anoop
'''
from mrjob.job import MRJob
import string

class WordFrequency(MRJob):
    def mapper(self, key, line):
        words = line.split()
        for word in words:
            word = word.lower().strip(string.punctuation + string.whitespace)
            yield word, 1
    
    def reducer(self, word, count):
        yield word, sum(count)


if __name__ == '__main__':
    WordFrequency.run()
