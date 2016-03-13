'''
Problem: Preprocess Social Network Data to
perform find Degrees of Separation

Data: Marvel_graph.txt
'''
import sys

with open("Preprocessed_Graph.txt", "w") as out:
    with open("Marvel-graph.txt") as fh:
        for line in fh:
            fields = line.split()
            hero = fields[0]
            connections = fields[1:]
            distance = 9999
            color = "WHITE"
            
            if hero == sys.argv[1]:
                distance = 0
                color = "GRAY" 
                
            edges = ','.join(connections)
            l = '|'.join((hero,edges, str(distance), color))
            l = l + '\n'
            out.write(l)
    fh.close()
out.close() 