from mrjob.job import MRJob
from mrjob.step import MRStep

'''
mrjob: write map reduce code/run Hadoop Streaming jobs in python
'''

class movieRatings(MRJob):
        #define mr phase
	def steps(self):return [        #define the mapper and reducer
                        MRStep(mapper=self.mapper,
                        reducer=self.reducer)]
        
	#breaks up input line and returns key value pair. 
	#get ratings data
	def mapper(self, _, line):
		(userID, movieID, rating, timestamp) = line.split('/t')
		yield rating, 1

	#takes each key and list of values
	#returns key and the sum of values associated w/ key	
	def reducer(self, key, values):
		yield key, sum(values)

#entry point for running job on command line
if __name__ == '__main__':
	movieRatings.run()
