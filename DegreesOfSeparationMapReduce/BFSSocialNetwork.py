from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node:
    def __init__(self):
        self.characterid = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'
    
    def fromLine(self, line):
        fields = line.split('|')
        if len(fields) == 4:
            self.characterid = fields[0]
            self.distance = int(fields[2])
            self.color = fields[3]
            self.connections = fields[1].split(',')
        
    def getLine(self):
        return '|'.join((self.characterid, ','.join(self.connections), str(self.distance), self.color))
        
class BFSSocialNetwork(MRJob):

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(BFSSocialNetwork, self).configure_options()
        self.add_passthrough_option(
            '--target', help="ID of character we are searching for")

        
    def mapper(self, key, line):
        node = Node()
        node.fromLine(line)
        if node.color == 'GRAY':
            for connection in node.connections:
                vnode = Node()
                vnode.characterid = connection
                vnode.color = 'GRAY'
                vnode.distance = node.distance + 1
                if (self.options.target == connection):
                    counterName = ("Target ID " + connection +
                        " was hit with distance " + str(vnode.distance))
                    self.increment_counter('Degrees of Separation',
                    counterName, 1)
                yield connection, vnode.getLine()    
            node.color = 'BLACK'
            
        yield node.characterid, node.getLine()
        
    def reducer(self, key, values ):
        edges = []
        distance = 9999
        color = 'WHITE'
        
        for value in values:
            node = Node()
            node.fromLine(value)
            if (len(node.connections) > 0):
                edges.extend(node.connections)
            if (node.distance < distance):
                distance = node.distance

            if ( node.color == 'BLACK' ):
                color = 'BLACK'

            if ( node.color == 'GRAY' and color == 'WHITE' ):
                color = 'GRAY'
                
        node = Node()
        node.characterID = key
        node.distance = distance
        node.color = color
        node.connections = edges

        yield key, node.getLine()
         

if __name__ == '__main__':
    BFSSocialNetwork.run()