import json
import datetime
import time
import csv
import requests
from bs4 import BeautifulSoup as BS
import pytz
import lxml

# add to list from categories.csv
ignored_categories = []

session = requests.Session()


# ###### Uncomment to run with proxies   ###############################


# start_with_proxy = input('Run with proxy?(y/n):')
#
# if start_with_proxy.strip() == 'y':
#     proxy = {
#         "http": "http://demoend:test123@35.230.150.209:3128/",
#         "https": "http://demoend:test123@35.230.150.209:3128"
#     }
# elif start_with_proxy.strip() == 'n':
#     proxy = None
# session.proxies.update(proxy)
#########################################################################


def request(url, retry=5):
    """Getting HTML"""
    try:
        response = session.get(url)
    except Exception as ex:
        time.sleep(3)
        print(f'{retry=},{url=},{ex=}')
        if retry:
            return request(url, retry=retry - 1)
        else:
            raise
    else:
        return response.text


def save_links_to_csv(url='https://www.costco.co.uk/sitemap_uk_product.xml', name='links'):
    """Importing links from XML page to CSV"""
    xml_page = request(url=url)
    links = BS(xml_page, 'lxml').find_all('loc')
    with open(f'{name}.csv', 'w') as csv_file:
        spam = csv.writer(csv_file)
        for n, link in enumerate(links):
            spam.writerow([n + 1, link.get_text()])


def iter_links(source='data'):
    """Link iteration from XML or CSV"""
    if '.csv' in source:
        with open(source) as file:
            links = csv.reader(file)
            for link in links:
                print(link)
                if link[1][25:].split('/')[0] in ignored_categories:
                    continue
                html_page = request(url=link[1])
                yield html_page
    else:
        xml_page = request(url='https://www.costco.co.uk/sitemap_uk_product.xml')
        links = BS(xml_page, 'lxml').find_all('loc')
        for link in links:
            if link[25:].split('/')[0] in ignored_categories:
                continue
            print(link.get_text())
            html_page = request(url=link.get_text())
            yield html_page


def parse_data(page):
    """Parse items"""
    soup = BS(page, 'lxml')
    all_json_data = soup.find_all('script', {'type': 'application/ld+json'})

    if len(list(all_json_data)) < 2:
        return None
    json_data = all_json_data[1].get_text()
    data_raw = json.loads(json_data)

    try:
        image_url = soup.find('div', class_="product-image")['data-product-img-url']
    except:
        image_url = None
    try:
        category = soup.find('ol', class_='breadcrumb').find_all('li')[-1].a.get('title')
    except:
        category = None
    try:
        brand = data_raw['brand']["name"]
    except:
        brand = None
    try:
        availability = data_raw['offers'].get('availability').replace('http://schema.org/', '')
    except:
        availability = None
    data = {
        'item': {
            'source': 'UK_Costco',
            'sku': data_raw.get('sku'),
            'name': data_raw.get("name"),
            'currency': 'GBP',
            'category': category,
            'brand': brand,
            'country': 'UK',
            'availability': availability,
            'url': data_raw.get("@id"),
            'upc': data_raw.get("gtin8"),
            'image_url': image_url
        },
        'price': {
            'price': data_raw['offers'].get('price'),
            'observed_date': str(pytz.utc.localize(datetime.datetime.utcnow()))
        }
    }

    return data


def main():
    all_objects = []
    try:
        save_links_to_csv()
        for page in iter_links(source='links.csv'):
            get_data = parse_data(page)
            if get_data:
                all_objects.append(get_data)
            print(get_data)
    except Exception as ex:
        print(ex)
    finally:
        with open(f'output_json/data-{str(datetime.datetime.now())[:-7]}.json', 'w', encoding='utf-8') as json_file:
            json.dump(all_objects, json_file, indent=4, separators=(',', ': '))


if __name__ == '__main__':
    main()
