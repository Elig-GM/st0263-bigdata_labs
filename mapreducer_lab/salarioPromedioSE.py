from mrjob.job import MRJob

class salarioPromedioSE(MRJob):

    def mapper(self, _, line):
        idemp, sececon, salary, year = line.split(',')
        try:
            salary = float(salary)
        except ValueError:
            pass
        else:
            yield sececon, salary


    def reducer (self, sececon, salaries):
        sum=0
        count =0
        for s in salaries:
            sum += s
            count += 1

        yield sececon, sum/count

if __name__ == '__main__':
    salarioPromedioSE.run()