import sys

from pprint import pprint
from ingestion.DelimitedParent import DelimitedParent

class Keywords(DelimitedParent):
    def run(self):
        """Process the data file for this type

        return: string
        """
        lstKeywordsLinkHeader = [
            "tmdb_id",
            "keyword_id"
        ]

        lstKeywordsHeader = [
            "keyword_id",
            "keyword"
        ]

        self.output(lstKeywordsLinkHeader, "movies_metadata_keywords.csv")
        self.output(lstKeywordsHeader, "keywords.csv")

        self.registerResponse("movies_metadata_keywords.csv", "movies_metadata_keywords")
        self.registerResponse("keywords.csv", "keywords")

        dicKeywordsColMap = {value:key for key, value in enumerate(lstKeywordsHeader)}
        dicKeywordsLinkColMap = {value:key for key, value in enumerate(lstKeywordsLinkHeader)}

        for dicLine in self.iterate():
            for dicSubline in dicLine["keywords"]:
                lstKeywordsOutput = [None] * len(lstKeywordsHeader)
                lstKeywordsLinkOutput = [None] * len(lstKeywordsLinkHeader)

                if not self.objDups.isDup("keywords", str(dicSubline["id"]) + ":" + dicSubline["name"]):
                    lstKeywordsOutput[dicKeywordsColMap["keyword_id"]] = dicSubline["id"]
                    lstKeywordsOutput[dicKeywordsColMap["keyword"]] = dicSubline["name"]

                    lstKeywordsLinkOutput[dicKeywordsLinkColMap["tmdb_id"]] = dicLine["id"]
                    lstKeywordsLinkOutput[dicKeywordsLinkColMap["keyword_id"]] = dicSubline["id"]

                    self.output(lstKeywordsOutput, "keywords.csv")
                    self.output(lstKeywordsLinkOutput, "movies_metadata_keywords.csv")

        self.closeAll()

        self.returnResponse()
