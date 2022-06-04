import json
import random
from typing import List, Any
from datetime import datetime, timedelta

CONFIGURATION_FILE = "./src/generators/config.json"


class SubscriptionElement(object):
    def __init__(self, field: str, value: Any, element_type: str, operator: str, constraints: dict):
        self.field = field
        self.value = value
        self.element_type = element_type
        self.operator = operator
        self.constraints = constraints

    def __str__(self) -> str:
        if self.element_type == "string":
            return f'({self.field}, {self.operator}, "{self.value}")'
        elif self.element_type == "double":
            precision = self.constraints["precision"]
            return f'({self.field}, {self.operator}, {self.value:.{precision}f})'
        elif self.element_type == "date":
            date_format = self.constraints["date_format"]
            return f'({self.field}, {self.operator}, {datetime.strftime(self.value, date_format)})'

        return f'({self.field}, {self.operator}, {self.value})'

    def to_json(self):
        value = self.value
        if self.element_type == "double":
            precision = self.constraints["precision"]
            value = round(value, precision)
        elif self.element_type == "date":
            value = int(datetime.timestamp(value))

        return json.dumps({
            "field": self.field,
            "operator": self.operator,
            "value": value
        })


class SubscriptionElementGenerator(object):
    def __init__(self, field: str, element_type: str, operators: List[str], constraints: dict):
        self.field = field
        self.element_type = element_type
        self.operators = operators
        self.constraints = constraints

    def generate_element(self, operator=None) -> SubscriptionElement:
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

        if not operator:
            operator = random.choice(self.operators)

        return SubscriptionElement(self.field, value, self.element_type, operator, self.constraints)


class Subscription(object):
    def __init__(self):
        self.elements = []

    def __str__(self) -> str:
        return '{' + '; '.join(str(element) for element in self.elements) + '}'

    def add_element(self, element: SubscriptionElement) -> None:
        self.elements.append(element)


class SubscriptionGenerator(object):
    def __init__(self, number_of_subscriptions: int, configuration: dict):
        self.number_of_subscriptions = number_of_subscriptions
        self.fields = [field for field in configuration["fields"]]
        self.configuration = configuration

    def generate_subscriptions(self) -> List[Subscription]:
        subscriptions = [Subscription() for _ in range(self.number_of_subscriptions)]

        current_index = 0

        for field in self.fields:
            frequency = self.configuration["fields"][field]["subscription_constraints"]["frequency"]

            eq_percentage = None
            if "eq_percentage" in self.configuration["fields"][field]["subscription_constraints"]:
                eq_percentage = self.configuration["fields"][field]["subscription_constraints"]["eq_percentage"]

            if frequency > 0:
                number_of_elements_to_generate = int(frequency * self.number_of_subscriptions / 100)
                min_number_of_elements_with_eq_to_generate = 0
                if eq_percentage:
                    min_number_of_elements_with_eq_to_generate = int(
                        eq_percentage * number_of_elements_to_generate / 100)

                element_type = self.configuration["fields"][field]["type"]
                operators = self.configuration["types"][element_type]["operators"]
                constraints = self.configuration["fields"][field]["value_constraints"]

                for i in range(number_of_elements_to_generate):
                    current_element = None
                    if i < min_number_of_elements_with_eq_to_generate:
                        current_element = SubscriptionElementGenerator(field, element_type, operators,
                                                                       constraints).generate_element("=")
                    elif i < number_of_elements_to_generate:
                        current_element = SubscriptionElementGenerator(field, element_type, operators,
                                                                       constraints).generate_element()

                    subscriptions[current_index].add_element(current_element)
                    current_index += 1
                    if current_index == self.number_of_subscriptions:
                        current_index = 0

        random.shuffle(subscriptions)
        return subscriptions


def generate_subscriptions(number_of_subscriptions: int) -> List[Subscription]:
    config = json.loads(open(CONFIGURATION_FILE).read())

    subscription_generator = SubscriptionGenerator(number_of_subscriptions, config)
    return subscription_generator.generate_subscriptions()
