import os
from io import StringIO
from typing import List, Union, Optional
from xml.etree import ElementTree as et

from xyw_eyes.rss.rss import RSS2


def merge_rss(name: str, files: Union[List[str], str], rss: Optional[RSS2] = None) -> None:
    """
    合并多个rss文件，可以通过传入RSS2对象自定义新rss文件的channel属性，没有传入时默认使用第一个文件的channel属性
    :param name: 新rss文件的保存地址
    :param files: 待合并rss文件的地址列表
    :param rss: 新rss文件的RSS2实例
    :return:
    """
    rss_items = []
    if isinstance(files, str):
        files = [files]
    for file in files:
        if not os.path.isfile(file):
            raise ValueError('file "{}" does not exists'.format(file))
        rss_items.append(et.parse(file).findall('.//item'))
    if rss is not None:
        rss = et.parse(StringIO(rss.to_xml()))
    else:
        rss = et.parse(files[0])
        root = rss.getroot()
        channel = root.find('channel')
        for item in rss.findall('.//item'):
            channel.remove(item)
    items = [item for items in rss_items for item in items]
    root = rss.getroot()
    for item in items:
        root.find('channel').append(item)
    rss.write(name, encoding='utf-8', xml_declaration=True)


if __name__ == '__main__':
    rss = RSS2(
        title='标题',
        link='https://www.douyu.com/',
        description='描述'
    )
    rss.set_build_time_now()
    merge_rss('douyu.xml', ['../../rss_spider/xml/douyu.xml', '../../rss_spider/xml/156xe.xml'], rss)
