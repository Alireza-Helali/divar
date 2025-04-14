from dis import dis
from math import dist
import scrapy
import json
import datetime


class DivarSpiderSpider(scrapy.Spider):
    name = 'divar_spider'

    @staticmethod
    def time_threshold():
        return datetime.date.today() - datetime.timedelta(days=7)

    @staticmethod
    def compare_date(epoch):
        return DivarSpiderSpider.time_threshold() < datetime.datetime.fromtimestamp(int(epoch)/1000000.0).date()

    def start_requests(self):
        with open('/home/alireza/crawler/crawler-master/divar/cities.json', 'r') as f:
            cities = json.load(f)
            for city in cities:
                url = "https://api.divar.ir/v8/postlist/w/search"
                payload = {
                    "city_ids": [city],
                    "source_view": "FILTER",
                    "disable_recommendation": False,
                    "map_state": {
                        "camera_info": {
                            "bbox": {}
                        }
                    },
                    "search_data": {
                        "form_data": {
                            "data": {
                                "category": {
                                    "str": {
                                        "value": "residential-sell"
                                    }
                                }
                            }
                        }
                    },
                    "server_payload": {
                        "@type": "type.googleapis.com/widgets.SearchData.ServerPayload"
                    }
                }

                payload = json.dumps(payload)
                yield scrapy.Request(
                    url=url, 
                    method='POST',
                    body=payload,
                    headers={'Content-Type': 'application/json'},
                    callback=self.parse,
                    meta={'city': city}
                )

    def parse(self, response):
        city = response.meta.get('city')
        data = json.loads(response.body)
        pagination = data.get('pagination')
        has_next_page = pagination.get("has_next_page")
        last_post_date_epoch = data.get('action_log').get('server_side_info').get('info').get('last_post_date_epoch')
        last_post_date = pagination.get("data").get("last_post_date")
        search_uid = pagination.get("data").get("search_uid")
        tokens = data.get('action_log').get('server_side_info').get('info').get('posts_metadata')
        if DivarSpiderSpider.compare_date(last_post_date_epoch):
            page = pagination.get("data").get('page')
            next_page = int(page) + 1
            for token in tokens:
                post_token = token.get('token')
                yield scrapy.Request(
                    url=f'https://api.divar.ir/v8/posts-v2/web/{post_token}', callback=self.parse_post, meta={'city': city})
            if has_next_page:
                payload_next_page = {
                    "city_ids": [
                        city
                    ],
                    "pagination_data": {
                        "@type": "type.googleapis.com/post_list.PaginationData",
                        "last_post_date": last_post_date,
                        "page": next_page,
                        "layer_page": next_page,
                        "search_uid": search_uid
                    },
                    "disable_recommendation": 0,
                    "map_state": {
                        "camera_info": {
                        "bbox": {}
                        }
                    },
                    "search_data": {
                        "form_data": {
                        "data": {
                            "category": {
                            "str": {
                                "value": "residential-sell"
                            }
                            }
                        }
                        },
                        "server_payload": {
                        "@type": "type.googleapis.com/widgets.SearchData.ServerPayload",
                        "additional_form_data": {
                            "data": {
                            "sort": {
                                "str": {
                                "value": "sort_date"
                                }
                            }
                            }
                        }
                        }
                    }
                }
                url = "https://api.divar.ir/v8/postlist/w/search"
                payload = json.dumps(payload_next_page)
                yield scrapy.Request(
                    url=url, 
                    method='POST',
                    body=payload,
                    headers={'Content-Type': 'application/json'},
                    callback=self.parse,
                    meta={'city': city}
                )

    def parse_post(self, response): 
        result = {}
        detail = json.loads(response.body)
        sections = detail.get('sections')
        webengage = detail.get('webengage')
        widgets = detail.get('widgets')

        result['business_type'] = webengage.get('business_type')
        result['price'] = webengage.get('price')
        result["city"] = webengage.get("city")
        result['district'] = webengage.get("district")
        result["business-type"] = webengage.get("cat_1")

        for section in sections:
            format = section.get('section_name')
            widgets = section.get("widgets")
            
            if format == 'MAP':
                for widget in widgets:
                    widget_type = widget.get("widget_type")
                    if widget_type == "MAP_ROW":
                        location_data = widget.get("data")
                        if location_data:
                            location = location_data.get("location")
                            if location:
                                exact_data = location.get("exact_data")
                                if exact_data:
                                    point = exact_data.get("point")
                                    if point is not None:
                                        result["latitude"] = point["latitude"]
                                        result["longitude"] = point["longitude"]
            elif format == "LIST_DATA":
                for widget in widgets:
                    widget_type = widget.get("widget_type")
                    if widget_type == "GROUP_INFO_ROW":
                        items = widget.get("data").get("items")
                        for item in items:
                            result[item["title"]] = item["value"]
            elif format == "LIST_DATA":
                widget_type = widget.get("widget_type")
                if widget_type == "UNEXPANDABLE_ROW":
                    items = widget.get("data")
                    result[item["title"]] = item["value"]

        yield result
