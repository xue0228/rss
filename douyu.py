"""
爬取斗鱼开播信息
斗鱼第三方接口：http://open.douyucdn.cn/api/RoomApi/room/{room_id}
"""
import os

from xyw_eyes.spider import Spider
from xyw_eyes.rss import RSS2, RSSItem, Guid, div, img, parse_string_to_datetime


class DouYu(Spider):
    name = 'douyu'
    start_urls = [
        '9500',
        '142488',
        '48699',
        '208114',
        '93589',
    ]
    request_delay = 5

    async def init(self):
        self.rss = RSS2(
            title='斗鱼',
            link='https://www.douyu.com/directory/myFollow',
            description='使用斗鱼第三方api接口监控关注主播的开播信息'
        )

    async def request_middlewares(self, request):
        request.url = 'http://open.douyucdn.cn/api/RoomApi/room/' + request.url
        return request

    async def response_filter_rule(self, response):
        if response.status != 200:
            return False
        data = await response.json()
        if data['error'] or data['data']['room_status'] == '2':
            return False
        return True

    async def parse(self, response):
        data = await response.json()
        self.logger.info(data)
        return {
            'title': '开播：' + data['data']['owner_name'],
            'description': img(data['data']['room_thumb']) + div(data['data']['room_name']),
            'pubDate': data['data']['start_time'],
            'link': 'https://www.douyu.com/' + data['data']['room_id']
        }

    async def item_pipeline(self, item):
        self.rss.items.append(
            RSSItem(
                title=item['title'],
                description=item['description'],
                link=item['link'],
                guid=Guid(
                    guid=item['link'] + item['pubDate'],
                    isPermaLink=False
                ),
                pubDate=parse_string_to_datetime(item['pubDate'])
            )
        )

    async def end(self) -> None:
        os.chdir(os.path.dirname(__file__))
        self.rss.set_build_time_now()
        await self.rss.async_write('./xml/' + self.name + '.xml')


if __name__ == '__main__':
    douyu = DouYu()
    douyu.run()
