from mrjob.job import MRJob
from mrjob.step import MRStep

class Avg_City_Temp(MRJob):   
    #(_, line) tells MRJob to ignore the key and take each line of the document as the value.
    def mapper(self,_, line):
        #split words based on colon into two seperate variables
        line_arr = line.split(':')
        city= line_arr[0]
        #converts temperature to float
        temp = float(line_arr[1])
        # Will Map (Sydney, (20 Degrees, 1 assigned to each entry)
        yield city, (temp, 1)
    
    def reducer_combiner(self, city, temperature):
        #assigning values for avgerage and number to increment by
        avg= 0
        num= 0
        #splitting the temp and count into from the value to use to solve the average
        for temp, count in temperature:
            avg = (avg*num + temp*count)/(num + count)
            #number increments by 1
            num += count
        # return eg Sydney, 19.66..
        return city, avg
      
    def reducer(self, city, avg):
        #linking reducer combiner so we can reduce the final results
        (city, avg) = self.reducer_combiner(city, avg)
        #ouputting the final result rounded to 2 decimal places
        yield city, round(avg,2)

if __name__ == '__main__':
    Avg_City_Temp.run()