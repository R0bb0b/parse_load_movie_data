import sys

from pprint import pprint
from ingestion.DelimitedParent import DelimitedParent

class Ratings(DelimitedParent):
    def run(self):
        """Process the data file for this type

        return: string
        """
        lstHeader = [
            "movie_id",
            "user_id",
            "rating",
            "timestamp"
        ]

        self.output(lstHeader, "ratings.csv")

        self.registerResponse("ratings.csv", "ratings")

        dicColMap = {value:key for key, value in enumerate(lstHeader)}

        for dicLine in self.iterate():
            strUniqueKey = str(dicLine["userId"]) + ":" + str(dicLine["movieId"]) + ":" + str(dicLine["rating"]) + ":" + str(dicLine["timestamp"])

            if not self.objDups.isDup("ratings", strUniqueKey):
                lstOutput = [None] * len(lstHeader)

                lstOutput[dicColMap["movie_id"]] = dicLine["movieId"]
                lstOutput[dicColMap["user_id"]] = dicLine["userId"]
                lstOutput[dicColMap["rating"]] = dicLine["rating"]
                lstOutput[dicColMap["timestamp"]] = dicLine["timestamp"]

                self.output(lstOutput, "ratings.csv")

        self.closeAll()

        self.returnResponse()
