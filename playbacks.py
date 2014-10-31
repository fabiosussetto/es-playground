from __future__ import division, unicode_literals
from functools import wraps


from random import random, choice, randint, sample
import json
from timeit import Timer
from bisect import bisect_right
from datetime import datetime
from elasticsearch import helpers as es_helpers
from elasticsearch.client import Elasticsearch


from faker import Faker
fake = Faker()


ES_HTTP_AUTH = 'elastic:AS8V2S27'

ES_NODES = [
    {'host': '10.2.0.201', 'port': 9200, 'http_auth': ES_HTTP_AUTH},
    {'host': '10.2.0.201', 'port': 9201, 'http_auth': ES_HTTP_AUTH}
]

ES = Elasticsearch(hosts=ES_NODES)

INDEX_NAME = 'playback_stats'
DOC_TYPE = 'playback'
CRF_INDEX_NAME = '121~fr'
POSTS_INDEX_NAME = 'posts'


with open('./data/movies.json') as movie_fp:
    MOVIES = json.load(movie_fp)


POST_MAPPING = {
    'properties': {
        'title': {
            'index': 'analyzed',
            'type': 'string',
        },
        'description': {
            'index': 'analyzed',
            'type': 'string',
        },
        'tags': {
            'index': 'not_analyzed',
            'type': 'string',
        }
    }
}


def generate_post_entry():
    tags = ['jazz', 'rock', 'alternative', 'country', 'techno', 'house', 'classical']
    return {
        'title': fake.sentence(nb_words=randint(4, 10)),
        'description': '. '.join(fake.paragraphs(nb=randint(1, 5))),
        'tags': sample(tags, randint(1, 3))
    }


def populate_posts(num):
    def action_generator():
        for _ in xrange(num):
            yield {
                '_index': POSTS_INDEX_NAME,
                '_type': 'posts',
                '_source': generate_post_entry()
            }

    actions = action_generator()
    es_helpers.bulk(client=ES, actions=actions, stats_only=True, raise_on_error=True)
    ES.indices.refresh(index=POSTS_INDEX_NAME)


def update_mapping():
    ES.indices.close(index=POSTS_INDEX_NAME)
    ES.indices.put_mapping(index=POSTS_INDEX_NAME, doc_type='post', body=POST_MAPPING)
    ES.indices.open(index=POSTS_INDEX_NAME)


def timeit_decorator(the_func):
    @wraps(the_func)
    def my_timeit(*args, **kwargs):
        output_container = []
        def wrapper():
            output_container.append(the_func(*args, **kwargs))
        timer = Timer(wrapper)
        delta = timer.timeit(1)
        my_timeit.last_execution_time = delta
        return output_container.pop()

    return my_timeit


def aggregate_by_hours_script():
    query = {
        "aggregations": {
            "by_hour": {
                "terms": {
                    "script": "doc['datetime'].date.hourOfDay"
                }
            }
        }
    }

    res = ES.search(index=INDEX_NAME, body=query)
    return res


def aggregate_by_hours_term():
    query = {
        "aggregations": {
            "by_hour": {
                "terms": {
                    "field": "hour_of_day"
                }
            }
        }
    }

    res = ES.search(index=INDEX_NAME, body=query)
    return res


def aggregate_by_rating_per_country():
    query = {
        "aggregations": {
            "by_country": {
                "terms": {
                    "field": "country"
                },
                "aggregations": {
                    "by_rating": {
                        "avg": {
                            "field": "movie.rank"
                        }
                    }
                }
            }
        }
    }

    res = ES.search(index=INDEX_NAME, body=query)
    return res


def random_datetime():
    year = 2014
    weighted_months = [[m, 1] for m in range(1, 12)]
    weighted_months[5][1] = 2
    weighted_months[6][1] = 3

    weighted_days = [[d, 1] for d in range(1, 31)]
    days_weights = [(9, 3), (10, 3), (11, 3)]

    for d, w in days_weights:
        entry = next((e for e in weighted_days if e[0] == d))
        entry[1] = w

    weighted_hours = [[h, 1] for h in range(9, 24)]
    hours_weights = [(15, 2), (16, 2), (20, 3), (21, 4), (22, 5), (23, 3)]

    for h, w in hours_weights:
        entry = next((e for e in weighted_hours if e[0] == h))
        entry[1] = w

    def _generate():
        month = weighted_choice(weighted_months)
        day = weighted_choice(weighted_days)
        minutes = choice(range(59))
        seconds = choice(range(59))
        return datetime(year, month, day, minutes, seconds)

    while True:
        try:
            return _generate()
        except ValueError:
            pass


def random_movie():
    weighted_movies = [(m, 1/m['rank']) for m in MOVIES]
    return weighted_choice(weighted_movies)


def weighted_choice(entries):
    values = [v[0] for v in entries]
    weights = [v[1] for v in entries]
    random_index = weighted_choice_index(weights=weights)

    return values[random_index]


def weighted_choice_index(weights):
    totals = []
    running_total = 0

    for w in weights:
        running_total += w
        totals.append(running_total)

    rnd = random() * running_total
    return bisect_right(totals, rnd)


def generate_playback_entry():
    event_types = [('start', 6), ('stop', 4)]
    countries = [
        ('UK', 4),
        ('France', 3),
        ('Spain', 3),
        ('USA', 2),
        ('Brazil', 2),
        ('Italy', 2),
        ('Germany', 1),
        ('Belgium', 1),
        ('Portugal', 1),
        ('Greece', .5)
    ]

    event_datetime = random_datetime()

    return {
        'datetime': event_datetime,
        'hour_of_day': event_datetime.hour,
        'event_type': weighted_choice(event_types),
        'country': weighted_choice(countries),
        'movie': random_movie()
    }


def create_index():
    ES.indices.create(index=INDEX_NAME)


def bulk_actions_generator(num):
    for _ in xrange(num):
        yield {
            '_index': INDEX_NAME,
            '_type': DOC_TYPE,
            '_source': generate_playback_entry()
        }

@timeit_decorator
def populate_index(num):
    actions = bulk_actions_generator(num)

    es_helpers.bulk(client=ES, actions=actions, stats_only=True, raise_on_error=True)
    ES.indices.refresh(index=INDEX_NAME)


def crf_analyze_field(field, text):
    return ES.indices.analyze(index=CRF_INDEX_NAME, body=text, field=field)

