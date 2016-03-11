'''
Problem: 
    Find the minimum temperature recorded in the year 1800
from data thats compiled from across weather stations at a couple 
of locations on planet earth.

Data:
    weather.csv
     
Author:anoop
'''
from mrjob.job import MRJob

class MinTempeartureYear1800(MRJob):
    
    def makeFahrenheit(self, temperature):
        celsius = float(temperature)/10.0
        fahrenheit = celsius * 1.8 +32.0
        return fahrenheit
        
    def mapper(self, key, line):
        (stationid, date, type, data, x, y, z, w ) = line.split(',')
        if type == 'TMIN':
            temperature = self.makeFahrenheit(data)
            yield stationid, temperature
    
    def reducer(self, stationid, temps):
        yield stationid, min(temps)
    
if __name__ == '__main__':
    MinTempeartureYear1800.run()