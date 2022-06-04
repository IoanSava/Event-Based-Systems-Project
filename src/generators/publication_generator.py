import json
import random
from typing import List, Any
from datetime import datetime, timedelta

CONFIGURATION_FILE = "./src/generators/config.json"


class PublicationElement(object):
    def __init__(self, field: str, value: Any, element_type: str, constraints: dict):
        self.field = field
        self.value = value
        self.element_type = element_type
        self.constraints = constraints

    def __str__(self) -> str:
        if self.element_type == "string":
            return f'({self.field}, "{self.value}")'
        elif self.element_type == "double":
            precision = self.constraints["precision"]
            return f'({self.field}, {self.value:.{precision}f})'
        elif self.element_type == "date":
            date_format = self.constraints["date_format"]
            return f'({self.field}, {datetime.strftime(self.value, date_format)})'

        return f'({self.field}, {self.value})'

    def to_json(self):
        value = self.value
        if self.element_type == "double":
            precision = self.constraints["precision"]
            value = round(value, precision)
        elif self.element_type == "date":
            value = int(datetime.timestamp(value))

        return json.dumps({
            "field": self.field,
            "value": value
        })


class PublicationElementGenerator(object):
    def __init__(self, field: str, element_type: str, constraints: dict):
        self.field = field
        self.element_type = element_type
        self.constraints = constraints

    def generate_element(self) -> PublicationElement:
        value = None

        if "values" in self.constraints:
            value = random.choice(self.constraints["values"])
        elif self.element_type == "double":
            lower = float(self.constraints["lower"])
            upper = float(self.constraints["upper"])
            precision = self.constraints["precision"]
            value = round(random.uniform(lower, upper), precision)
        elif self.element_type == "date":
            start = datetime.strptime(self.constraints["start"], self.constraints["date_format"])
            end = datetime.strptime(self.constraints["end"], self.constraints["date_format"])
            diff = end - start
            value = start + timedelta(days=random.randrange(diff.days))

        return PublicationElement(self.field, value, self.element_type, self.constraints)


class Publication(object):
    def __init__(self):
        self.elements = []

    def __str__(self) -> str:
        return '{' + '; '.join(str(element) for element in self.elements) + '}'

    def add_element(self, element: PublicationElement) -> None:
        self.elements.append(element)


class PublicationGenerator(object):
    def __init__(self, number_of_publications: int, configuration: dict):
        self.number_of_publications = number_of_publications
        self.fields = [field for field in configuration["fields"]]
        self.configuration = configuration

    def __generate_publication(self) -> Publication:
        publication = Publication()

        for field in self.fields:
            field_type = self.configuration["fields"][field]["type"]
            field_constraints = self.configuration["fields"][field]["value_constraints"]
            element = PublicationElementGenerator(field, field_type, field_constraints).generate_element()
            publication.add_element(element)

        return publication

    def generate_publications(self) -> List[Publication]:
        return [self.__generate_publication() for _ in range(self.number_of_publications)]


def generate_publications(number_of_publications: int) -> List[Publication]:
    config = json.loads(open(CONFIGURATION_FILE).read())

    publication_generator = PublicationGenerator(number_of_publications, config)
    return publication_generator.generate_publications()
