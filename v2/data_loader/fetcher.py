import csv
import os


class FetcherCSV:
    def __init__(self):
        base_dir = os.path.dirname(__file__)

        self.path = os.path.join(base_dir, "./data/tweets_injected3.csv")
        self.path_weapon = os.path.join(base_dir, "../data/weapon_list.txt")

    def fetcher_from_csv(self):
        data = []
        try:
            with open(self.path, encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(row)
            return data
        except Exception as e:
            print(str(e))

    def read_txt(self):
        try:
            with open(self.path_weapon, "r", encoding='utf-8-sig') as file:
                return file.read()
        except Exception as e:
            print(str(e))
