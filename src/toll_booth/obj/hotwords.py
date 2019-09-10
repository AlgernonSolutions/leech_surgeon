import os

import boto3
from algernon import AlgObject
from botocore.exceptions import ClientError


class ClinicalHotword(AlgObject):
    def __init__(self, word_category, world_value):
        self._word_category = word_category
        self._word_value = world_value

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['word_category'], json_dict['word_value'])

    @property
    def word_category(self):
        return self._word_category

    @property
    def word_value(self):
        return self._word_value


class ClinicalHotwords(AlgObject):
    def __init__(self, hotwords):
        self._hotwords = hotwords

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['hotwords'])

    @classmethod
    def retrieve(cls, id_source):
        resource = boto3.resource('dynamodb')
        table = resource.Table(os.environ['CLIENT_DATA_TABLE_NAME'])
        identifier = '#clinical_hotwords#'
        try:
            response = table.get_item(Key={'identifier_stem': identifier, 'sid_value': id_source})
            base_words = response['Item']
        except KeyError:
            response = table.get_item(Key={'identifier_stem': identifier, 'sid_value': 'Algernon'})
            base_words = response['Item']
        hotwords = []
        for category, words in base_words.items():
            for word in words:
                hotwords.append(ClinicalHotword(category, word))
        return cls(hotwords)

    def __iter__(self):
        return iter([x.word_value for x in self._hotwords])
