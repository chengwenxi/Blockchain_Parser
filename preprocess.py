"""Pull data from geth and parse it into mongo."""
from preprocessing.crawler import Crawler

print("Booting processes")

c = Crawler.Crawler()
