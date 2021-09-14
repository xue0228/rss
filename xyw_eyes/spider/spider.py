import time
from abc import abstractmethod, ABCMeta
from typing import Optional
from threading import Thread
import asyncio
from aiohttp import ClientResponse
from dataclasses import dataclass

from xyw_eyes.spider.request import Request
from xyw_eyes.spider.item import Item
from xyw_eyes.logger import get_logger


@dataclass
class QueueNum:
    """
    队列计数类，用于统计各队列中任务的执行情况，为终止协程子进程提供依据
    """
    # 任务总数
    total: int = 0
    # 成功的任务数
    success: int = 0
    # 失败的任务数
    fail: int = 0

    def check(self) -> bool:
        """
        检查任务是否全部完成
        :return:
        """
        return True if (self.success + self.fail) == self.total else False

    def add_total(self) -> None:
        """
        任务总数加一
        :return:
        """
        self.total += 1

    def add_success(self) -> None:
        """
        成功的任务数加一
        :return:
        """
        if (self.success + self.fail) <= (self.total - 1):
            self.success += 1
        else:
            raise RuntimeError('over total')

    def add_fail(self) -> None:
        """
        失败的任务数加一
        :return:
        """
        if (self.success + self.fail) <= (self.total - 1):
            self.fail += 1
        else:
            raise RuntimeError('over total')


class Spider(metaclass=ABCMeta):
    """
    爬虫类，用于快速爬取网络数据
    """
    # 爬虫名称
    # name: str
    # 起始爬取链接
    # start_urls: Union[str, dict[str]]
    # request和item处理失败后重试的次数
    retry_times = 10
    # 每一次request请求之间的最小时间间隔
    request_delay = 0

    def __init__(self):
        """
        初始化实例，主要为类属性检查以及队列的创建
        """
        if not hasattr(self, 'name'):
            raise RuntimeError('name can not be empty')
        if not hasattr(self, 'start_urls'):
            raise RuntimeError('start_urls can not be empty')
        if not isinstance(self.start_urls, (str, list)):
            raise TypeError('start_urls must be string or list of string')
        if isinstance(self.start_urls, list) and len(self.start_urls) != 0:
            for url in self.start_urls:
                if not isinstance(url, str):
                    raise TypeError('start_urls must be string or list of string')
        if not (isinstance(self.retry_times, int) and self.retry_times >= 0):
            raise TypeError('retry_times must be integer no less than 0')
        if not (isinstance(self.request_delay, int) and self.request_delay >= 0):
            raise TypeError('request_delay must be integer no less than 0')

        self.logger = get_logger('spider-' + self.name)

        self.logger.info('开始初始化队列')
        # 用于控制协程并发网络请求的数量
        self._semaphore = asyncio.Semaphore(500)
        # 创建异步队列
        self.request_queue = asyncio.Queue(maxsize=0)
        self.response_queue = asyncio.Queue(maxsize=0)
        self.item_queue = asyncio.Queue(maxsize=0)
        # 创建任务计数实例
        self._request_num = QueueNum()
        self._response_num = QueueNum()
        self._item_num = QueueNum()
        self.logger.info('初始化队列完成')

    async def _download(self, request: Request) -> ClientResponse:
        """
        实际发送请求，并向返回结果中添加Request属性，用于传递请求信息
        :param request:
        :return:
        """
        self.logger.info('开始下载request：%s %s' % (request.method, request.url))
        async with self._semaphore:
            async with request.request() as resp:
                resp.request = request
                # 此处需要直接使用read()方法将数据下载下来
                await resp.read()
        self.logger.info('下载request成功：%s %s %d' % (request.method, request.url, resp.status))
        return resp

    @staticmethod
    def _start_loop(loop) -> None:
        """
        启动一个一直运行的事件循环
        :param loop:
        :return:
        """
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def _process_request(self, request: Request) -> None:
        """
        用于添加到协程事件循环的request处理函数
        :param request:
        :return:
        """
        # 过滤request请求，不符合项直接跳过处理，request处理成功数加一
        try:
            self.logger.info('开始过滤request：%s %s' % (request.method, request.url))
            filter_result = await self.request_filter_rule(request)
            self.logger.info('过滤request成功：%s %s' % (request.method, request.url))
        except Exception:
            self.logger.error('过滤request失败：%s %s' % (request.method, request.url), exc_info=True)
            self._request_num.add_fail()
            return

        if not filter_result:
            self._request_num.add_success()
            return

        # 检查request请求重试次数，超过限值的直接跳过处理，request处理失败加一
        if request.retry_times > self.retry_times:
            self._request_num.add_fail()
            return

        # 通过中间件对request请求进行处理，例如添加代理等
        try:
            self.logger.info('开始处理request中间件：%s %s' % (request.method, request.url))
            real_request = await self.request_middlewares(request)
            self.logger.info('处理request中间件成功：%s %s' % (request.method, request.url))
        except Exception:
            self.logger.error('处理request中间件失败：%s %s' % (request.method, request.url), exc_info=True)
            self._request_num.add_fail()
            return

        try:
            # 发送request请求，下载网络数据
            response = await self._download(real_request)

            # 向response队列插入任务
            self.logger.info('开始向response队列插入任务：%s %s' % (request.method, request.url))
            await self.response_queue.put(response)
            self.logger.info('向response队列插入任务成功：%s %s' % (request.method, request.url))

            # request处理成功数加一
            self._request_num.add_success()
        except Exception:
            # 下载失败时修改request请求，retry_times加一
            self.logger.error('第%d次下载失败：%s %s' % (request.retry_times + 1, request.method, request.url), exc_info=True)
            request.increase_retry_times()

            # 将修改后的request请求重新加入request队列，同时request处理失败加一
            await self.request_queue.put(request)
            self._request_num.add_fail()
        finally:
            # 因为没有使用join()方法，所以此行代码意义不大
            self.request_queue.task_done()

    async def _process_response(self, response: ClientResponse) -> None:
        """
        用于添加到协程事件循环的response处理函数
        :param response:
        :return:
        """
        try:
            self.logger.info('开始过滤response：%s %s' % (response.request.method, response.request.url))
            filter_result = await self.response_filter_rule(response)
            self.logger.info('过滤response成功：%s %s' % (response.request.method, response.request.url))
        except Exception:
            self.logger.error('过滤response失败：%s %s' % (response.request.method, response.request.url), exc_info=True)
            self._response_num.add_fail()
            return

        if not filter_result:
            self._response_num.add_success()
            return

        try:
            self.logger.info('开始处理response中间件：%s %s' % (response.request.method, response.request.url))
            real_response = await self.response_middlewares(response)
            self.logger.info('处理response中间件成功：%s %s' % (response.request.method, response.request.url))
        except Exception:
            self.logger.error('处理response中间件失败：%s %s' % (response.request.method, response.request.url), exc_info=True)
            self._response_num.add_fail()
            return

        self.logger.info('开始解析response：%s %s' % (real_response.request.method, real_response.request.url))
        try:
            data = await self.parse(real_response)
        except Exception:
            self.logger.error(
                '解析response失败：%s %s' % (real_response.request.method, real_response.request.url),
                exc_info=True
            )
            self._response_num.add_fail()
            return
        self.logger.info('解析response成功：%s %s' % (real_response.request.method, real_response.request.url))

        self.response_queue.task_done()
        if isinstance(data, dict):
            self.logger.info('开始向item队列插入任务：%s %s' % (real_response.request.method, real_response.request.url))
            await self.item_queue.put(Item(data, request=real_response.request))
            self.logger.info('向item队列插入任务成功：%s %s' % (real_response.request.method, real_response.request.url))
        self._response_num.add_success()

    async def _process_item(self, item: Item) -> None:
        """
        用于添加到协程事件循环的item处理函数
        :param item:
        :return:
        """
        if item.retry_times > self.retry_times:
            self._item_num.add_fail()
            return
        try:
            self.logger.info('开始处理item：%s %s' % (item.request.method, item.request.url))
            await self.item_pipeline(item.data)
            self.logger.info('处理item成功：%s %s' % (item.request.method, item.request.url))
            self._item_num.add_success()
        except Exception:
            self.logger.error(
                '第%d次处理item失败：%s %s' % (item.retry_times, item.request.method, item.request.url),
                exc_info=True
            )
            item.increase_retry_times()
            await self.item_queue.put(item)
            self._item_num.add_fail()
        finally:
            self.item_queue.task_done()

    async def async_run(self) -> None:
        """
        运行爬虫的实际协程函数
        :return:
        """
        # 运行自定义的初始化函数
        await self.init()

        # 根据类属性start_urls初始化起始请求
        self.logger.info('开始初始化起始请求')
        start_urls = self.start_urls
        if isinstance(start_urls, str):
            start_urls = [start_urls]
        for url in start_urls:
            await self.request_queue.put(Request(url))
        self.logger.info('初始化起始请求完成')

        # 启动协程子进程，用于运行协程任务
        self.logger.info('启动协程子进程')
        thread_loop = asyncio.new_event_loop()
        process_thread = Thread(target=self._start_loop, args=(thread_loop,), daemon=True)
        process_thread.start()
        self.logger.info('协程子进程启动完成')

        # 用于记录上次发送request请求的时间
        last_time = 0

        # 向协程事件循环中提交任务的主循环
        while True:
            # response队列中有任务时从队列中取出一个加入协程循环
            if self.response_queue.qsize():
                asyncio.run_coroutine_threadsafe(self._process_response(await self.response_queue.get()), thread_loop)
                self._response_num.add_total()

            # item队列中有任务时从队列中取出一个加入协程循环
            if self.item_queue.qsize():
                asyncio.run_coroutine_threadsafe(self._process_item(await self.item_queue.get()), thread_loop)
                self._item_num.add_total()

            # request队列中有任务时从队列中取出一个加入协程循环
            if self.request_queue.qsize():
                # 请求间隔小于限值时不从队列取下载任务
                if time.time() - last_time < self.request_delay:
                    continue
                asyncio.run_coroutine_threadsafe(self._process_request(await self.request_queue.get()), thread_loop)
                self._request_num.add_total()
                last_time = time.time()

            # 检查各队列以及协程循环中各任务的完成情况，全部完成时结束主循环
            if self._item_num.check() \
                    and self._response_num.check() \
                    and self._request_num.check() \
                    and self.item_queue.qsize() == 0 \
                    and self.response_queue.qsize() == 0 \
                    and self.request_queue.qsize() == 0:
                self.logger.info('共处理request %d 次，其中成功 %d 次，失败 %d 次'
                            % (self._request_num.total, self._request_num.success, self._request_num.fail))
                self.logger.info('共处理response %d 次，其中成功 %d 次，失败 %d 次'
                            % (self._response_num.total, self._response_num.success, self._response_num.fail))
                self.logger.info('共处理item %d 次，其中成功 %d 次，失败 %d 次'
                            % (self._item_num.total, self._item_num.success, self._item_num.fail))
                break

        # 运行自定义的收尾函数
        await self.end()
        
        # 清零计数
        self._request_num = QueueNum()
        self._response_num = QueueNum()
        self._item_num = QueueNum()

    async def init(self) -> None:
        """
        爬虫开始运行前的初始化，可以进行登录之类的操作
        :return:
        """
        pass

    async def end(self) -> None:
        """
        爬虫全部爬取完成后执行的操作
        :return:
        """
        pass

    async def request_middlewares(self, request: Request) -> Request:
        """
        用于自定义请求，例如修改请求头、添加代理等
        :param request:
        :return:
        """
        return request

    async def response_middlewares(self, response: ClientResponse) -> ClientResponse:
        """
        用于修改返回
        :param response:
        :return:
        """
        return response

    async def request_filter_rule(self, request: Request) -> bool:
        """
        request过滤规则，返回Fasle的request将不会被处理
        默认全部放行
        :param request:
        :return:
        """
        return True

    async def response_filter_rule(self, response: ClientResponse) -> bool:
        """
        response过滤规则，返回False的response将不会被处理
        默认放行[200， 300)的状态码
        :param response:
        :return:
        """
        return True if 200 <= response.status < 300 else False

    @abstractmethod
    async def parse(self, response: ClientResponse) -> Optional[dict]:
        """
        解析请求结果，输出dict，也可以在此处向各队列提交任务
        :param response:
        :return:
        """
        pass

    @abstractmethod
    async def item_pipeline(self, item: dict):
        """
        处理解析出的数据，例如存储或邮件提醒
        :param item:
        :return:
        """
        pass

    def run(self) -> None:
        """
        启动爬虫
        :return:
        """
        # event_loop = asyncio.get_event_loop()
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        try:
            event_loop.run_until_complete(self.async_run())
        finally:
            event_loop.close()
