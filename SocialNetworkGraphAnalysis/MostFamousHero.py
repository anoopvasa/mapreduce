'''
Problem: Find the most popular super hero from a netwrked graph of
    fictional super heroes

Data: Super Hero Data thats been cross referenced/associated with 
    other characters in different types of material

Author: anoop
'''
    
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostFamousHero(MRJob):
    
    def configure_options(self):
        super(MostFamousHero, self).configure_options()
        self.add_file_option('--items',help='Path to Marvel names')
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_ref,
                   reducer_init=self.reducer_init ),
            MRStep(reducer=self.reducer_find_famous)
        ]
    
    def mapper(self, key, line):
        network = line.split()
        yield int(network[0]), len(network)-1
        
    def reducer_init(self):
        self.character_names = {}
        with open("Marvel-names.txt") as fh:
            for line in fh:
                ch = line.split('"')
                self.character_names[int(ch[0])] = ch[1]        
    
    def reducer_count_ref(self, character, numOfRef):
        yield  None,(sum(numOfRef), self.character_names[character])
    
    def reducer_find_famous(self, key, value):
        yield max(value)

if __name__ == '__main__':
    MostFamousHero.run()