import csv


class FetcherCSV:
    def __init__(self, path='./data/tweets_injected3.csv'):
        self.path=path
        self.path_weapon='./data/weapon_list.txt'


    def fetcher_from_csv(self):

        data = []
        try:
            with open(self.path, encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                # print(reader)
                for row in reader:
                    data.append(row)
                    # print(row)
            # print(data)
            return data
        except Exception as e:
            print(str(e))


    def read_txt(self):
        with open(self.path_weapon, "r") as file:
            return file.read()



