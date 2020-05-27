import sys

from pprint import pprint
from ingestion.DelimitedParent import DelimitedParent

class MoviesMetadata(DelimitedParent):
    def __init__(self, *args, **kwargs):
        """constructor
        """
        self.lstCsvExceptions = [
            ["- Written by Ørnås", "- Written by Ørnås\""],
            ["British soldiers force a recently captured IRA terrorist to cooperate", "\"British soldiers force a recently captured IRA terrorist to cooperate"],
            ["try to wrap up her case once and for all.", "try to wrap up her case once and for all.\""],
            ["Third film of the Mardock Scramble series.", "\"Third film of the Mardock Scramble series."],
            ["horrifying affair when it is hit by a shark avalanche.", "horrifying affair when it is hit by a shark avalanche.\""],
            ["A group of skiers are terrorized during spring break by some", "\"A group of skiers are terrorized during spring break by some"]
        ]

        super().__init__(*args, **kwargs)

    def run(self):
        """Process the data file for this type

        return: string
        """
        lstMoviesMetadataHeader = [
            "adult",
            "collection_id",
            "budget",
            "homepage",
            "tmdb_id",
            "imdb_id",
            "original_language",
            "original_title",
            "overview",
            "popularity",
            "poster_path",
            "release_date",
            "revenue",
            "runtime",
            "status",
            "tagline",
            "title",
            "video",
            "vote_average",
            "vote_count"
        ]

        lstCollectionsHeader = [
            "collection_id",
            "name",
            "poster_path",
            "backdrop_path"
        ]

        lstMoviesMetadataGenreHeader = [
            "tmdb_id",
            "genre_id"
        ]

        lstGenresHeader = [
            "genre_id",
            "name"
        ]

        lstMoviesMetadataProductionCompaniesHeader = [
            "tmdb_id",
            "production_company_id"
        ]

        lstProductionCompaniesHeader = [
            "production_company_id",
            "name"
        ]

        lstMoviesMetadataProductionCountriesHeader = [
            "tmdb_id",
            "production_country"
        ]

        lstProductionCountriesHeader = [
            "production_country",
            "name"
        ]

        lstMoviesMetadataLanguagesHeader = [
            "tmdb_id",
            "language_abbr"
        ]

        lstLanguagesHeader = [
            "language_abbr",
            "language"
        ]

        self.output(lstMoviesMetadataHeader,                    "movies_metadata.csv")
        self.output(lstCollectionsHeader,                       "collections.csv")
        self.output(lstMoviesMetadataGenreHeader,               "movies_metadata_genres.csv")
        self.output(lstGenresHeader,                            "genres.csv")
        self.output(lstMoviesMetadataProductionCompaniesHeader, "movies_metadata_production_companies.csv")
        self.output(lstProductionCompaniesHeader,               "production_companies.csv")
        self.output(lstMoviesMetadataProductionCountriesHeader, "movies_metadata_production_countries.csv")
        self.output(lstProductionCountriesHeader,               "production_countries.csv")
        self.output(lstMoviesMetadataLanguagesHeader,           "movies_metadata_languages.csv")
        self.output(lstLanguagesHeader,                         "languages.csv")

        self.registerResponse("movies_metadata.csv",                        "movies_metadata")
        self.registerResponse("collections.csv",                            "collections")
        self.registerResponse("movies_metadata_genres.csv",                 "movies_metadata_genres")
        self.registerResponse("genres.csv",                                 "genres")
        self.registerResponse("movies_metadata_production_companies.csv",   "movies_metadata_production_companies")
        self.registerResponse("production_companies.csv",                   "production_companies")
        self.registerResponse("movies_metadata_production_countries.csv",   "movies_metadata_production_countries")
        self.registerResponse("production_countries.csv",                   "production_countries")
        self.registerResponse("movies_metadata_languages.csv",              "movies_metadata_languages")
        self.registerResponse("languages.csv",                              "languages")

        dicMoviesMetadataHeaderColMap =                     {value:key for key, value in enumerate(lstMoviesMetadataHeader)}
        dicCollectionsHeaderColMap =                        {value:key for key, value in enumerate(lstCollectionsHeader)}
        dicMoviesMetadataGenresHeaderColMap =               {value:key for key, value in enumerate(lstMoviesMetadataGenreHeader)}
        dicGenresHeaderColMap =                             {value:key for key, value in enumerate(lstGenresHeader)}
        dicMoviesMetadataProductionCompaniesHeaderColMap =  {value:key for key, value in enumerate(lstMoviesMetadataProductionCompaniesHeader)}
        dicProductionCompaniesHeaderColMap =                {value:key for key, value in enumerate(lstProductionCompaniesHeader)}
        dicMoviesMetadataProductionCountriesHeaderColMap =  {value:key for key, value in enumerate(lstMoviesMetadataProductionCountriesHeader)}
        dicProductionCountriesHeaderColMap =                {value:key for key, value in enumerate(lstProductionCountriesHeader)}
        dicMoviesMetadataLanguagesHeaderColMap =            {value:key for key, value in enumerate(lstMoviesMetadataLanguagesHeader)}
        dicLanguagesHeaderColMap =                          {value:key for key, value in enumerate(lstLanguagesHeader)}

        for dicLine in self.iterate():
            try:
                lstOutput = [None] * len(lstMoviesMetadataHeader)
                lstOutput[dicMoviesMetadataHeaderColMap["adult"]] =                 dicLine["adult"]
                lstOutput[dicMoviesMetadataHeaderColMap["collection_id"]] =         dicLine["belongs_to_collection"]["id"] if "id" in dicLine["belongs_to_collection"] else None
                lstOutput[dicMoviesMetadataHeaderColMap["budget"]] =                dicLine["budget"]
                lstOutput[dicMoviesMetadataHeaderColMap["homepage"]] =              dicLine["homepage"]
                lstOutput[dicMoviesMetadataHeaderColMap["tmdb_id"]] =               dicLine["id"]
                lstOutput[dicMoviesMetadataHeaderColMap["imdb_id"]] =               dicLine["imdb_id"] #requires transformation to remove the first two letters
                lstOutput[dicMoviesMetadataHeaderColMap["original_language"]] =     dicLine["original_language"]
                lstOutput[dicMoviesMetadataHeaderColMap["original_title"]] =        dicLine["original_title"]
                lstOutput[dicMoviesMetadataHeaderColMap["overview"]] =              dicLine["overview"]
                lstOutput[dicMoviesMetadataHeaderColMap["popularity"]] =            dicLine["popularity"]
                lstOutput[dicMoviesMetadataHeaderColMap["poster_path"]] =           dicLine["poster_path"]
                lstOutput[dicMoviesMetadataHeaderColMap["release_date"]] =          dicLine["release_date"]
                lstOutput[dicMoviesMetadataHeaderColMap["revenue"]] =               dicLine["revenue"]
                lstOutput[dicMoviesMetadataHeaderColMap["runtime"]] =               dicLine["runtime"]
                lstOutput[dicMoviesMetadataHeaderColMap["status"]] =                dicLine["status"]
                lstOutput[dicMoviesMetadataHeaderColMap["tagline"]] =               dicLine["tagline"]
                lstOutput[dicMoviesMetadataHeaderColMap["title"]] =                 dicLine["title"]
                lstOutput[dicMoviesMetadataHeaderColMap["video"]] =                 dicLine["video"]
                lstOutput[dicMoviesMetadataHeaderColMap["vote_average"]] =          dicLine["vote_average"]
                lstOutput[dicMoviesMetadataHeaderColMap["vote_count"]] =            dicLine["vote_count"]
                self.output(lstOutput, "movies_metadata.csv")
            except Exception as error:
                raise ValueError(error)

            if "id" in dicLine["belongs_to_collection"]:
                lstOutput = [None] * len(lstCollectionsHeader)
                lstOutput[dicCollectionsHeaderColMap["collection_id"]] =    dicLine["belongs_to_collection"]["id"]
                lstOutput[dicCollectionsHeaderColMap["name"]] =             dicLine["belongs_to_collection"]["name"]
                lstOutput[dicCollectionsHeaderColMap["poster_path"]] =      dicLine["belongs_to_collection"]["poster_path"]
                lstOutput[dicCollectionsHeaderColMap["backdrop_path"]] =    dicLine["belongs_to_collection"]["backdrop_path"]
                self.output(lstOutput, "collections.csv")

            for dicSubline in dicLine["genres"]:
                lstOutput = [None] * len(lstMoviesMetadataGenreHeader)
                lstOutput[dicMoviesMetadataGenresHeaderColMap["tmdb_id"]] =                             dicLine["id"]
                lstOutput[dicMoviesMetadataGenresHeaderColMap["genre_id"]] =                            dicSubline["id"]
                self.output(lstOutput, "movies_metadata_genres.csv")

                if not self.objDups.isDup("genres", str(dicSubline["id"]) + ":" + str(dicSubline["name"])):
                    lstOutput = [None] * len(lstGenresHeader)
                    lstOutput[dicGenresHeaderColMap["genre_id"]] =                                      dicSubline["id"]
                    lstOutput[dicGenresHeaderColMap["name"]] =                                          dicSubline["name"]
                    self.output(lstOutput, "genres.csv")

            for dicSubline in dicLine["production_companies"]:
                lstOutput = [None] * len(lstMoviesMetadataProductionCompaniesHeader)
                lstOutput[dicMoviesMetadataProductionCompaniesHeaderColMap["tmdb_id"]] =                dicLine["id"]
                lstOutput[dicMoviesMetadataProductionCompaniesHeaderColMap["production_company_id"]] =  dicSubline["id"]
                self.output(lstOutput, "movies_metadata_production_companies.csv")

                if not self.objDups.isDup("production_companies", str(dicSubline["id"]) + ":" + str(dicSubline["name"])):
                    lstOutput = [None] * len(lstProductionCompaniesHeader)
                    lstOutput[dicProductionCompaniesHeaderColMap["production_company_id"]] =            dicSubline["id"]
                    lstOutput[dicProductionCompaniesHeaderColMap["name"]] =                             dicSubline["name"]
                    self.output(lstOutput, "production_companies.csv")

            for dicSubline in dicLine["production_countries"]:
                lstOutput = [None] * len(lstMoviesMetadataProductionCountriesHeader)
                lstOutput[dicMoviesMetadataProductionCountriesHeaderColMap["tmdb_id"]] =                dicLine["id"]
                lstOutput[dicMoviesMetadataProductionCountriesHeaderColMap["production_country"]] =     dicSubline["iso_3166_1"]
                self.output(lstOutput, "movies_metadata_production_countries.csv")

                if not self.objDups.isDup("production_countries", str(dicSubline["iso_3166_1"]) + ":" + str(dicSubline["name"])):
                    lstOutput = [None] * len(lstProductionCountriesHeader)
                    lstOutput[dicProductionCountriesHeaderColMap["production_country"]] =               dicSubline["iso_3166_1"]
                    lstOutput[dicProductionCountriesHeaderColMap["name"]] =                             dicSubline["name"]
                    self.output(lstOutput, "production_countries.csv")

            for dicSubline in dicLine["spoken_languages"]:
                lstOutput = [None] * len(lstMoviesMetadataLanguagesHeader)
                lstOutput[dicMoviesMetadataLanguagesHeaderColMap["tmdb_id"]] =                          dicLine["id"]
                lstOutput[dicMoviesMetadataLanguagesHeaderColMap["language_abbr"]] =                    dicSubline["iso_639_1"]
                self.output(lstOutput, "movies_metadata_languages.csv")

                if not self.objDups.isDup("languages", str(dicSubline["iso_639_1"]) + ":" + str(dicSubline["name"])):
                    lstOutput = [None] * len(lstLanguagesHeader)
                    lstOutput[dicLanguagesHeaderColMap["language_abbr"]] =                              dicSubline["iso_639_1"]
                    lstOutput[dicLanguagesHeaderColMap["language"]] =                                   dicSubline["name"]
                    self.output(lstOutput, "languages.csv")

        self.closeAll()

        self.returnResponse()
