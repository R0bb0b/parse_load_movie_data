import sys

from pprint import pprint
from ingestion.DelimitedParent import DelimitedParent

class Links(DelimitedParent):
    def run(self):
        """Process the data file for this type

        return: string
        """
        lstHeader = [
            "movie_id",
            "tmdb_id",
            "imdb_id"
        ]

        self.output(lstHeader, "links.csv")

        self.registerResponse("links.csv", "links")

        dicColMap = {value:key for key, value in enumerate(lstHeader)}

        for dicLine in self.iterate():
            if not self.objDups.isDup("links", str(dicLine["imdbId"]) + ":" + str(dicLine["tmdbId"])):
                lstOutput = [None] * len(lstHeader)

                lstOutput[dicColMap["movie_id"]] = dicLine["movieId"]
                lstOutput[dicColMap["tmdb_id"]] = dicLine["tmdbId"]
                lstOutput[dicColMap["imdb_id"]] = dicLine["imdbId"]

                self.output(lstOutput, "links.csv")

        self.closeAll()

        self.returnResponse()
