import numpy as np
from scipy.cluster.hierarchy import fclusterdata


import json
import os
import re
import subprocess
from collections import OrderedDict
from pymongo import GEO2D, TEXT, MongoClient
import itertools


from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.filter import filter_mongo_schema_namespaces, init_filtered_schema
from pymongo_schema.tosql import mongo_schema_to_mapping

import scipy.cluster.hierarchy as hac

from helper import read_database_config,querry_lsh
from unionabililty import cal_set_unionability

def mydist(p1, p2):
    diff = p1 - p2
    return np.vdot(diff, diff) ** 0.5


class DataUnion:
    
    schema = {}

    def run(self):
        pass
    
    def gen_distance_matrix(self):
        pass

    def compute_distance_(self, data_set1, data_set2):
        pass

    def gen_matchings(self, data_set1, data_set2):
        data_1, atttribute_name_1 = data_set1 
        data_2, atttribute_name_2 = data_set2 
        
        matchings = []

        atttributes_1 = {}
        atttributes_2 = {}
        
        for record in data_1:
            for att in atttribute_name_1:
                temp = atttributes_1.get(att)
                if temp and record.get(att):
                    temp.append(record.get(att))
                if not temp and record.get(att):
                    atttributes_1.set(att,[record.get(att)])

        for record in data_2:
            for att in atttribute_name_2:
                temp = atttributes_2.get(att)
                if temp and record.get(att):
                    temp.append(record.get(att))
                if not temp and record.get(att):
                    atttributes_2.set(att,[record.get(att)])

        #Gett attribute mappings
        mappings = {}

        max_c = 0

        for atttribute in atttribute_name_1:
            result = querry_lsh(atttribute, data_set2)
            if result and len(result)>0:
                for item in result:
                    score = cal_set_unionability(data_1.get(att), data_2.get(item))
                    mappings[(att, item)] = score
                
                max_c +=1

        best_c_mappings = []

        for i in range(1,max_c+1):
            best_c_mappings.append(find_best_c_align,i,mappings)

    def find_best_c_align(self, c, mappings):        
        possible_alligns = itertools.combinations(mappings.keys(), c)
        valid_allign = []

        for allign in possible_alligns:
            temp_first = [mapping[0] for mapping in allign]
            temp_second = [mapping[1] for mapping in allign]

            #The number of attribute must be equals (no a1 mapped to b1 and a2 mapped to b1)
            if len(set(temp_first)) == len(set(temp_second)):
                valid_allign.append(allign)

        best_score = -1
        best_mapping = []

        #Di kiem allign
        for allign in possible_alligns:
            current_score = 1
            for mapping in allign:
                current_score = current_score* mappings(mapping)
            if current_score > best_score:
                best_score = current_score
                best_mapping = allign

        return best_mapping

    def extractSchema(self):
        db_config = read_database_config()
        # extract schema from MongoDB
        connection_string = f"mongodb://{db_config['mongodb_host']}:{db_config['mongodb_port']}/"
        # Making connection
        mongo_client = MongoClient(
            connection_string, username=db_config['mongodb_username'], password=db_config['mongodb_password'])

        schema = extract_pymongo_client_schema(mongo_client)
        return schema

    def convertSchema(self):
        schema = self.extractSchema()

        mapping = mongo_schema_to_mapping(schema)

        dbs_name = [*mapping][0]

        self.schema = (dbs_name+"_schema", mapping[dbs_name])
        return True

    def cluster(self):
        X = np.random.randn(100, 2)

        fclust1 = hac.linkage(X, metric=mydist)

        print(X)


if __name__ == "__main__":
    data_union = DataUnion()
    data_union.cluster()



