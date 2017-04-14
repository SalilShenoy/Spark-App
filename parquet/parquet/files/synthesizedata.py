import csv


class synthesizeData(object):

	def createCSV(self):
		with open ('syn_dosage.csv', 'wb') as dosagecsv:
			fieldNames = ['Individual']
			dosageWriter = csv.DictWriter(dosagecsv, fieldnames=fieldNames)
			dosageWriter.writeheader()

			numIndividuals = 2000000
			data = ["%d" % int(i+1) for i in range(numIndividuals)]
			dosageWriter.writerows(data)


def main():
	sd = synthesizeData()
	sd.createCSV()

if __name__ == "__main__":
	main()
