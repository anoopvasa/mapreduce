'''
Problem: 
    Calculate word frequency of a book in the
    sorted order by chaining two MAP REDUCE Jobs

Data:
    Any book from PROJECT GUTENBERG
     
Author:anoop
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class WordFrequencySteps(MRJob):
    
    def steps(self):
        return [
        MRStep(mapper=self.mapper_get_words,
               reducer=self.reducer_count_words),
        MRStep(mapper=self.mapper_make_counts_key,
               reducer=self.reducer_output_words)
        ]
        
    def mapper_get_words(self, key, line):
        words = line.split()
        for word in words:
            word = word.lower().strip(string.punctuation + string.whitespace)
            yield word, 1
    
    def reducer_count_words(self, word, count):
        yield word, sum(count)
    
    def mapper_make_counts_key(self,word,count):
        yield '%04d'%int(count), word
    
    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    WordFrequencySteps.run()