from xyw_eyes.spider.request import Request


class Item:
    def __init__(self, data: dict, request: Request, retry_times: int = 0):
        if not isinstance(data, dict):
            raise TypeError('dict only')
        if not isinstance(retry_times, int):
            raise TypeError('int only')
        self.data = data
        self.request = request
        self.retry_times = retry_times

    def increase_retry_times(self):
        self.retry_times = self.retry_times + 1


if __name__ == '__main__':
    item = Item({'xue': 1}, 0)
    print(item)
