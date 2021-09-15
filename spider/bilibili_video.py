"""
爬取B站Up主视频投稿信息
"""
import datetime
import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(root_path)
sys.path.append(root_path)

from xyw_eyes.spider import Spider
from xyw_eyes.rss import RSS2, RSSItem, Guid, div, img


class Video(Spider):
    name = 'bilibili_video'
    start_urls = [
        '820583',
        '14232358',
        '25150941',
        '11336264',
        '28626598',
        '1958342',
        '3341680',
        '10462362'
    ]
    request_delay = 5

    async def init(self):
        self.rss = RSS2(
            title='B站视频投稿',
            link='https://www.bilibili.com/',
            description='关注B站Up主的最新视频投稿'
        )

    async def request_middlewares(self, request):
        request.url = 'https://api.bilibili.com/x/space/arc/search?mid={}&ps=10&tid=0&pn=1&order=pubdate&jsonp=jsonp'\
            .format(request.url)
        request.headers = {
            'Referer': 'https://space.bilibili.com/{}/'.format(request.url)
        }
        return request

    async def response_filter_rule(self, response):
        if response.status != 200:
            return False
        data = await response.json()
        if data['code']:
            return False
        return True

    async def parse(self, response):
        data = await response.json()
        self.logger.info(data)
        return {
            'data': data['data']['list']['vlist']
        }

    def video_link(self, item: dict):
        if item['bvid']:
            text = 'bvid={}'.format(item['bvid'])
        else:
            text = 'aid={}'.format(item['aid'])
        url = 'https://player.bilibili.com/player.html?{}&high_quality=1'.format(text)
        return '<iframe ' \
               'src="{}" ' \
               'width="650" height="477" scrolling="no" border="0" frameborder="no" framespacing="0" ' \
               'allowfullscreen="true"></iframe>'.format(url)

    async def item_pipeline(self, item):
        vlist = item['data']
        for item in vlist:
            link = 'https://www.bilibili.com/video/{}'.format(item['bvid']) if item['created'] > 1589990400 and item['bvid'] else 'https://www.bilibili.com/video/av{}'.format(item['aid'])
            self.rss.items.append(
                RSSItem(
                    title='【{}】 {}'.format(item['author'], item['title']),
                    description=img(item['pic']) + div(item['description']) + self.video_link(item),
                    link=link,
                    guid=Guid(
                        guid=link,
                        isPermaLink=False
                    ),
                    pubDate=datetime.datetime.fromtimestamp(item['created'])
                )
            )

    async def end(self) -> None:
        self.rss.set_build_time_now()
        await self.rss.async_write('./xml/' + self.name + '.xml')


if __name__ == '__main__':
    spider = Video()
    spider.run()
