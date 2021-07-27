import json
import datetime
import time
import csv
import requests
from bs4 import BeautifulSoup as BS
import pytz
import lxml

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
}

start_with_proxy = input('Run with proxy?(y/n):')
if start_with_proxy.strip() == 'y':
    proxy = {
        "http": "http://demoend:test123@35.230.150.209:3128/",
        "https": "http://demoend:test123@35.230.150.209:3128"
    }
elif start_with_proxy.strip() == 'n':
    proxy = None

link_to_all_categories_xml = 'https://www.costco.co.uk/sitemap_uk_category.xml'


def request(url, retry=5):
    """Getting HTML"""
    try:
        response = requests.get(url, headers=headers, proxies=proxy)
    except Exception as ex:
        time.sleep(3)
        print(f'{retry=},{url=},{ex=}')
        if retry:
            return request(url, retry=retry - 1)
        else:
            raise
    else:
        return response.text


def save_links_to_csv(url='https://www.costco.co.uk/sitemap_uk_product.xml'):
    """Importing links from XML page to CSV"""
    xml_page = request(url=url)
    links = BS(xml_page, 'lxml').find_all('loc')
    with open('links.csv', 'w') as csv_file:
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
                html_page = request(url=link[1])
                yield html_page
    else:
        xml_page = request(url='https://www.costco.co.uk/sitemap_uk_product.xml')
        links = BS(xml_page, 'lxml').find_all('loc')
        for link in links:
            print(link.get_text())
            html_page = request(url=link.get_text())
            yield html_page


def parse_data(page):
    """Parse items"""
    soup = BS(page, 'lxml')
    all_json_data = soup.find_all('script', {'type': 'application/ld+json'})
    if len(list(all_json_data)) < 2:
        if soup.find('ul', class_='product-listing product-grid') is None:  # There are no products in category
            return None
        else:  # page new product list
            links = soup.find('ul', class_='product-listing product-grid').find_all('div',
                                                                                    class_='product-name-container')
            for link in links:
                new_link = 'https://www.costco.co.uk' + link.find('a')['href']
                parse_data(request(new_link))
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
            data = parse_data(page)
            all_objects.append(data)
            print(data)
    except Exception as ex:
        print(ex)
    finally:
        with open('data_test_2.json', 'w', encoding='utf-8') as json_file:
            json.dump(all_objects, json_file, indent=4, allow_nan=False, separators=(',', ': '))


if __name__ == '__main__':
    main()
