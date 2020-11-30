from mrjob.job import MRJob

class salarioPromedioE(MRJob):

    def mapper(self, _, line):
        idemp, sececon, salary, year = line.split(',')
        try:
            salary = float(salary)
        except ValueError:
            pass
        else:
            yield idemp, salary


    def reducer (self, idemp, salaries):
        sum=0
        count =0
        for s in salaries:
            sum += s
            count += 1

        yield idemp, sum/count

if __name__ == '__main__':
    salarioPromedioE.run()