import sys

from pprint import pprint
from ingestion.DelimitedParent import DelimitedParent

class Credits(DelimitedParent):
    def run(self):
        """Process the data file for this type

        return: string
        """
        lstHeader = [
            "tmdb_id",
            "cast_id",
            "character",
            "credit_id",
            "department",
            "gender",
            "contributor_id",
            "job",
            "name",
            "cast_order",
            "profile_path"
        ]

        self.output(lstHeader, "credits.csv")

        self.registerResponse("credits.csv", "credits")

        dicColMap = {value:key for key, value in enumerate(lstHeader)}

        for dicLine in self.iterate():
            for dicSubline in dicLine["cast"]:
                lstOutput = [None] * len(lstHeader)

                lstOutput[dicColMap["tmdb_id"]] = dicLine["id"]
                lstOutput[dicColMap["cast_id"]] = dicSubline["cast_id"]
                lstOutput[dicColMap["character"]] = str(dicSubline["character"]).strip()
                lstOutput[dicColMap["credit_id"]] = dicSubline["credit_id"]
                lstOutput[dicColMap["gender"]] = dicSubline["gender"]
                lstOutput[dicColMap["contributor_id"]] = dicSubline["id"]
                lstOutput[dicColMap["job"]] = "Actor"
                lstOutput[dicColMap["name"]] = dicSubline["name"]
                lstOutput[dicColMap["cast_order"]] = dicSubline["order"]
                lstOutput[dicColMap["profile_path"]] = dicSubline["profile_path"]

                self.output(lstOutput, "credits.csv")

            for dicSubline in dicLine["crew"]:
                lstOutput = [None] * len(lstHeader)

                lstOutput[dicColMap["tmdb_id"]] = dicLine["id"]
                lstOutput[dicColMap["credit_id"]] = dicSubline["credit_id"]
                lstOutput[dicColMap["department"]] = dicSubline["department"]
                lstOutput[dicColMap["gender"]] = dicSubline["gender"]
                lstOutput[dicColMap["contributor_id"]] = dicSubline["id"]
                lstOutput[dicColMap["job"]] = dicSubline["job"]
                lstOutput[dicColMap["name"]] = dicSubline["name"]
                lstOutput[dicColMap["profile_path"]] = dicSubline["profile_path"]

                self.output(lstOutput, "credits.csv")

        self.closeAll()

        self.returnResponse()
