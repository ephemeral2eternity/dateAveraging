import csv

def loadCSV2List(csv_file_name):
    csvfile = open(csv_file_name, 'r')
    dictreader = csv.DictReader(csvfile)
    fields = dictreader.fieldnames
    # print fields

    data_list = []

    for row in dictreader:
        data_list.append(row)
        # print row

    return data_list


if __name__ == "__main__":
    csv_file = "D://Box Sync/Data processing/recom.csv"
    csv_data = loadCSV2List(csv_file)
