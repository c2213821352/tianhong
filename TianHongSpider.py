import json
import uuid

import utils.userAgent
from proxy import proxyIp
from utils import log
from utils.fetch import fetch, fetch_data

logger = log.log().getLogger()


class TianHongSpider:
    def __init__(self):
        self.new_proxy = proxyIp.proxyIp()

    def get_city_list(self):
        url = 'https://api.tianhong.cn/support-ms-app/store/city-list'
        headers = {
            # 'Host': 'api.tianhong.cn',
            'User-Agent': utils.userAgent.get_random_weixin_user_agent(),
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate,br',
            'Referer': 'https://servicewechat.com',
        }
        proxies = None
        for i in range(3):
            proxy = self.new_proxy.get_ip(i)
            if proxy is None:
                continue
            proxies = {'http': 'http://' + proxy, 'https': 'http://' + proxy}
            break
        return fetch('POST', url, json={}, headers=headers, proxies=proxies)

    """{'id': 4512273, 'name': '阿坝藏族羌族自治州', 'regionalismCode': '513200', 'longitudeInternational': 
    '37.9720684302883', 'latitudeInternational': '112.48190096873282', 'longitudeChina': '37.966951399051055', 
    'latitudeChina': '112.48186122971897', 'longitudeBaidu': '37.972758', 'latitudeBaidu': '112.48844', 
    'simplifiedIndex': 'A', 'longitudeGd': '102.224504', 'latitudeGd': '31.899427'}"""

    def get_area_list(self, city_info):
        url = "https://api.tianhong.cn/member-ms-app/address-v2/poi-around/list"
        longitude = city_info['longitudeChina'] or city_info['longitudeInternational'] or city_info['longitudeBaidu'] or \
                    city_info['longitudeGd']
        latitude = city_info['latitudeChina'] or city_info['latitudeInternational'] or city_info['latitudeBaidu'] or \
                   city_info['latitudeGd']

        if longitude is None or latitude is None:
            return []

        payload = {
            "longitude": longitude,
            "latitude": latitude,
            "businessType": "1"
        }
        headers = {
            # 'Host': 'api.tianhong.cn',
            'User-Agent': utils.userAgent.get_random_weixin_user_agent(),
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate,br',
            # 'Referer': 'https://servicewechat.com',
        }
        resp = fetch("POST", url, json=payload, headers=headers)
        return resp['poiInfoList']

    """
    {'province': '山西省', 'provinceCode': '31063', 'city': '太原市', 'cityCode': '31066', 'area': '尖草坪区', 'areaCode': 
    '4512118', 'poiCode': 'B0FFGK3U3Z', 'poiName': '西张小区', 'canDelivery': 1, 'longitude': '112.480849', 'latitude': 
    '37.958892', 'poiAddress': '柴村镇西张村'}
    """

    def get_store_list(self, city_code):
        url = "https://api.tianhong.cn/support-ms-app/store/near/list"
        payload = {"cityCode": city_code}
        headers = {
            # 'Host': 'api.tianhong.cn',
            'User-Agent': utils.userAgent.get_random_weixin_user_agent(),
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate,br',
            # 'Referer': 'https://servicewechat.com',
        }
        return fetch("POST", url, json=payload, headers=headers)

    def fetch_cate_list(self, store_info, parent_id):
        url = "https://dj.tianhong.cn/dj_product_api/v5/category/list-sale-category"
        payload = json.dumps({
            "storeDTO": {
                "storeList": [{
                    "djBusinessType": store_info['businessType'],
                    "lbsStoreCode": "C029",
                    "storeCode": store_info['storeCode'],
                    "selected": 1
                }]},
            "parentId": parent_id,
            "isExpandFirst": 0
        })

        headers = {
            "x-http-deviceuid": uuid.uuid1().hex,
            "x-http-devicetype": "miniapp",
            "x-http-version": "3.8.0",
            "content-type": "application/json",
            'User-Agent': utils.userAgent.get_random_weixin_user_agent(),
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate,br',
        }
        return fetch("POST", url, data=payload, headers=headers)

    """{'id': 903723, 'code': None, 'name': '肉蛋水产', 'desc': None, 'parentId': 0, 'treeLevel': 1, 'treePath': 
    '903723', 'categoryImageUrl': 'https://img1.tianhong.cn/upload/djp/image/defalut/2024/02/TYo59eU0l9r9DzYvqfSYde4k
    .jpeg', 'subList': [{'id': 904423, 'code': None, 'name': '羊肉 鹿肉', 'desc': None, 'parentId': 903723, 'treeLevel': 
    2, 'treePath': '903723/904423', 'categoryImageUrl': '', 'subList': [{'id': 919672, 'code': None, 'name': '鹿肉', 
    'desc': None, 'parentId': 904423, 'treeLevel': 3, 'treePath': '903723/904423/919672', 'categoryImageUrl': '', 
    'subList': []}]}]}"""

    def fetch_goods_list(self, store_info, cate_info):
        url = "https://dj.tianhong.cn/dj_product_api/v5/product/list-category-product"
        payload = {
            "storeDTO": {
                "lbsAddressCode": "31308",
                "storeList": [
                    {
                        "djBusinessType": store_info['businessType'],
                        "lbsStoreCode": "C029",
                        "storeCode": store_info['storeCode'],
                        "selected": 1
                    }
                ]
            },
            "categoryId": cate_info['cateLv3'],  # 919672,
            "deviceId": "",
            "pageIndex": 0,
            "pageSize": "10",
            "rankType": 1
        }
        headers = {
            "x-http-deviceuid": uuid.uuid1().hex,
            "x-http-devicetype": "miniapp",
            "xweb_xhr": "1",
            "x-http-version": "3.8.0",
            "content-type": "application/json",
            'User-Agent': utils.userAgent.get_random_weixin_user_agent(),
        }

        resp = fetch_data("POST", url, json=payload, headers=headers)
        result = resp['data']

        payload['pageIndex'] = 1

        while resp['pagination']['hasNext'] == 1:
            payload['pageIndex'] += 1
            headers['x-http-deviceuid'] = uuid.uuid1().hex
            headers['User-Agent'] = utils.userAgent.get_random_weixin_user_agent()

            resp = fetch_data("POST", url, json=payload, headers=headers)
            if resp['data'] is not None:
                result += resp['data']

        return result

    def fetch_goods_detail(self, product):
        url = "https://dj.tianhong.cn/dj_product_api/v5/product/detail"

        payload = json.dumps({
            "productId": product['productId'],  # 1001686,
            "storeCode": product['storeCode'],  # "01502",
            "djBusinessType": 4
        })
        headers = {
            "x-http-deviceuid": uuid.uuid1().hex,
            "x-http-devicetype": "miniapp",
            "xweb_xhr": "1",
            "x-http-version": "3.8.0",
            "content-type": "application/json",
            'User-Agent': utils.userAgent.get_random_weixin_user_agent(),
        }

        return fetch("POST", url, data=payload, headers=headers)

    def get_sku_detail(self, goods):
        return [{
            'stock': item['stockNum'],
            'name': goods['productName'],
            'skuId': item['skuId'],
            'itemId': goods['productId'],
            'sales': '',
            'imgUrl': ','.join(goods['imageList']),
            'storeId': goods['storeInfo']['storeCode'],
            'originalPrice': '',
            'realtimePrice': item['salePrice'],
            'enabled': goods['isSell'],
            'upc': goods['productCode'],
            'saleUnit': '',
            'priceUnit': '',
            'startWith': goods['buyMin'] if goods['buyMin'] else '',
            'saleStep': ''
        } for item in goods['skuItemList']
        ]
