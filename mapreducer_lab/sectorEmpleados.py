from mrjob.job import MRJob

class salarioPromedioE(MRJob):

    def mapper(self, _, line):
        idemp, sececon, salary, year = line.split(',')
        yield idemp, sececon


    def reducer (self, idemp, sececon):
        count =0
        for s in sececon:
            count += 1

        yield idemp, count

if __name__ == '__main__':
    salarioPromedioE.run()