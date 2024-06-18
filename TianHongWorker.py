import atexit
import datetime
import hashlib
import json
import os
import subprocess
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import oss2

from conf import config
from proxy import proxyIp
from tianhong.TianHongSpider import TianHongSpider
from utils import log, message

logger = log.log().getLogger()

auth = oss2.Auth(config.config().ossSpiderdataKey, config.config().ossSpiderdataSecret)
bucket = oss2.Bucket(
    auth, config.config().ossSpiderdataEndpoint, config.config().ossSpiderdataBucket
)


class TianHongWorker:
    def __init__(self):
        self.spider = TianHongSpider()

    def run(self):
        logger.info("TianHong Beginning")
        try:
            # 获取城市列表
            city_list = self.spider.get_city_list()
            city_code_list = [item['cityCode'] for item in city_list]
            store_list = []
            cate_list = []
            # 从城市列表中获取店铺列表
            with ThreadPoolExecutor(max_workers=5) as pool:
                area_result_list = pool.map(self.spider.get_store_list, city_code_list)
                for item in area_result_list:
                    for iitem in item:
                        if iitem['deleted'] == 0:
                            store_list.append(iitem)
            for store in store_list:
                cate_list = []
                sku_list = []
                # 获取店铺分类信息
                try:
                    cate_1_res_list = self.spider.fetch_cate_list(store, 0)
                    for cate_1 in cate_1_res_list:
                        cate_2_res_list = self.spider.fetch_cate_list(store, cate_1['id'])
                        for cate_2 in cate_2_res_list:
                            cate_3_res_list = self.spider.fetch_cate_list(store, cate_2['id'])
                            for cate_3 in cate_3_res_list:
                                cate = {
                                    'cateLv1': cate_1['id'],
                                    'cateLv2': cate_2['id'],
                                    'cateLv3': cate_3['id'],
                                    'cateLv1Name': cate_1['name'],
                                    'cateLv2Name': cate_2['name'],
                                    'cateLv3Name': cate_3['name'],
                                }
                                try:
                                    product_list = self.spider.fetch_goods_list(store, cate)
                                    with ThreadPoolExecutor(max_workers=5) as pool:
                                        if product_list is None:
                                            continue
                                        result = pool.map(self.spider.fetch_goods_detail, product_list)
                                        for item in result:
                                            part_sku_list = self.spider.get_sku_detail(item)
                                            for iitem in part_sku_list:
                                                sku_list.append(json.dumps({
                                                    **iitem,
                                                    **cate
                                                }, ensure_ascii=False))
                                except Exception as e:
                                    logger.error(repr(e))
                                    message.feishu(
                                        "天虹数据获取异常",
                                        f"storeID {store['storeCode']} cateLv3Id {cate['cateLv3']} cateLv3Name {cate['cateLv3Name']}\n原因 > {repr(e)}"
                                    )
                                ############################################################################
                                # cate_list.append(cate)
                                logger.info(
                                    f"storeID {store['storeCode']} cateLv3Id {cate['cateLv3']} cateLv3Name {cate['cateLv3Name']} : {len(sku_list)}"
                                )

                    # 分类获取结束
                    oss_text = '\n'.join(sku_list)
                    hl = hashlib.md5()
                    hl.update(str(int(time.time())).encode(encoding='utf-8'))
                    sign = hl.hexdigest()
                    file_name = f"TIANHONG-WXMINIAPP-{store['storeCode']}" + "-" + str(
                        datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + '-' + sign
                    with open(file_name, mode='w', encoding='utf8') as w:
                        w.write(oss_text)
                    # w.close()
                    # 文件压缩
                    file_name_zip = file_name + ".zip"
                    f = zipfile.ZipFile(file_name_zip, 'w', zipfile.ZIP_DEFLATED)
                    f.write(file_name)
                    f.close()
                    stats = os.stat(file_name_zip)
                    ret = bucket.put_object_from_file(file_name_zip, file_name_zip)
                    if ret.status != 200:
                        message.feishu(
                            "文件上传失败",
                            f"天虹 {store['storeCode']} {round(stats.st_size / 1024, 2)}"
                        )
                    logger.info(f"TianHong {store['storeCode']} upload complete")
                    # os.remove(file_name)
                    os.remove(file_name_zip)
                except Exception as e:
                    logger.exception(e)
                    message.feishu(
                        "天虹数据获取失败",
                        f"天虹 storeID : {store['storeCode']} ; storeName : {store['storeName']}\n"
                        f"原因 >\n  {e}"
                    )
        except Exception as e:
            logger.exception(e)
        logger.info("TianHong Paused")


if __name__ == '__main__':
    node_command = f"node {os.path.dirname(__file__)}/../js/app.js"
    process = subprocess.Popen(node_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    atexit.register(lambda: process.terminate())
    proxyIp.proxyIp().update_proxy_pool()
    worker = TianHongWorker()
    worker.run()
