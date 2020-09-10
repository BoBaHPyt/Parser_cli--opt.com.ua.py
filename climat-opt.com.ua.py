from asyncio import run, gather
from aiohttp import ClientSession
from lxml.html import fromstring, tostring
from csv import writer
from html2text import html2text
from json import load

from json_dump import open_df


DUMP_FILE = 'climat-opt.com.ua.json'
RESULT_FILE = 'climat-opt.com.ua.csv'
NUMS_THREAD = 50


async def get_page(url, **kwargs):
    """Асинхронная загрузка страницы"""
    async with ClientSession() as sess:
        async with sess.get(url, **kwargs) as req:
            if req.status == 200:
                return await req.text(errors='replace')
            else:
                return False


async def get_all_subcatalog_urls(catalog_url):
    """Парсит url'ы всех подкаталогов с каталога сайта"""
    content_page = await get_page(catalog_url)
    if not content_page:
        return

    document = fromstring(content_page)

    urls = document.xpath('//ul[@class="catalog category"]/li/a/@href')

    for i in range(len(urls)):
        urls[i] = 'https://climat-opt.com.ua' + urls[i]

    return urls


async def get_all_catalog_urls():
    """Парсит url'ы всех каталогов сайта"""
    url = 'https://climat-opt.com.ua/catalog'
    content_page = await get_page(url)
    if not content_page:
        return

    document = fromstring(content_page)

    urls = document.xpath('//ul[@class="catalog category"]/li/a/@href')

    for i in range(len(urls)):
        urls[i] = 'https://climat-opt.com.ua' + urls[i]

    return urls


async def get_product_urls_from_page(url_page):
    """Парсит все url'ы со страницы католога"""
    content_page = await get_page(url_page)
    if not content_page:
        return

    document = fromstring(content_page)

    urls = document.xpath('//div[@class="tovar_item"]/div/div[@class="name"]/a/@href')

    for i in range(len(urls)):
        urls[i] = 'https://climat-opt.com.ua' + urls[i]

    return urls


async def get_product_data(url):
    """Парсит карточку товара"""
    data = {'url': url}

    content_page = await get_page(url)
    if not content_page:
        return

    document = fromstring(content_page)

    images = document.xpath('//div[@class="fll"]/ul/li/a/@data-original')
    if images:
        for i in range(len(images)):
            images[i] = 'https://climat-opt.com.ua' + images[i]
    data['images'] = images

    name = html2text(''.join(document.xpath('//div[@class="flr"]/h1[@class="title title_ogr"]//text()')))
    assert name, url
    data['Название'] = name

    article = document.xpath('//div[@class="flr"]/div[@class="article item"]/span/text()')
    assert article, url
    data['Артикул'] = article[0]

    characteristics = html2text('\n'.join(document.xpath('//div[@class="fll wTxt"]/p[position()>2]//text()'))).\
            replace('\r', '').replace('\t', '').replace('  ', '')
    data['Характеристики'] = characteristics

    models = document.xpath('//table[@class="table_tovar table_item"]/tr[@class="sup_row"]/td[1]/div//text()')
    model_area = document.xpath('//table[@class="table_tovar table_item"]/tr[@class="sup_row"]/td[2]/text()')
    model_prices = document.xpath('//table[@class="table_tovar table_item"]/tr[@class="sup_row"]/td[3]/span/text()')

    data['models'] = models
    data['model_area'] = model_area
    data['model_prices'] = model_prices

    return data


async def get_all_product_urls():
    catalog_urls = await get_all_catalog_urls()

    subcatalog_urls = []
    answers = await gather(*[get_all_subcatalog_urls(url) for url in catalog_urls])
    for answer in answers:
        subcatalog_urls += answer

    product_urls = []
    answers = await gather(*[get_product_urls_from_page(url) for url in subcatalog_urls])
    for answer in answers:
        if answer:
            product_urls += answer

    return product_urls


async def main():
    product_urls = await get_all_product_urls()

    dump_file = open_df(DUMP_FILE)
    for i in range(0, len(product_urls), NUMS_THREAD):
        urls = product_urls[i: i + NUMS_THREAD] if i + NUMS_THREAD < len(product_urls) else product_urls[i:]

        answers = await gather(*[get_product_data(url) for url in urls])

        for answer in answers:
            if answer:
                dump_file.write(answer)
    dump_file.close()


def get_max_photo_length(data):
    max_length = 0
    for product in data:
        if len(product['images']) > max_length:
            max_length = len(product['images'])
    return max_length


def dump_to_csv():
    products_data = []
    with open(DUMP_FILE, 'r') as file:
        products_data = load(file)

    max_photo_length = get_max_photo_length(products_data)

    row_names = ['url'] + ['Фото товара'] * max_photo_length + ['Артикул', 'Название', 'Модель', 'Цена', 'Площадь', 'Характеристики']

    with open(RESULT_FILE, 'w') as file:
        csv_writer = writer(file)

        csv_writer.writerow(row_names)

        for product in products_data:
            for model_index in range(len(product['models'])):
                row = [''] * 7 + [''] * max_photo_length

                row[0] = product['url']
                for i in range(1, len(product['images']) + 1):
                    row[i] = product['images'][i - 1]

                row[max_photo_length + 1] = product['Артикул']
                row[max_photo_length + 2] = product['Название']
                row[max_photo_length + 3] = product['models'][model_index]
                row[max_photo_length + 4] = product['model_prices'][model_index] if len(product['model_prices']) > model_index else ''
                row[max_photo_length + 5] = product['model_area'][model_index] if len(product['model_area']) > model_index else ''
                row[max_photo_length + 6] = product['Характеристики'] if 'Характеристики' in product else ''

                csv_writer.writerow(row)


if __name__ == '__main__':
    if input('Parsing [y|n]').lower() == 'y':
        run(main())
    if input('Convert json to csv [y|n]').lower() == 'y':
        dump_to_csv()
