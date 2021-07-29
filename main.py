import json
import datetime
import time
import csv
import requests
from bs4 import BeautifulSoup as BS
import pytz

# Set to True to search all items
search_all = False

# If search_all False - add or remove categories (all types of catagories - in categories.csv)
selected_categories = (
    'Fitness-Well-Being',
    'Furniture-Home',
    'Mattresses-Beds'
    'Electronics-Security',
    'Computers',
    'Appliances',
    'Garden-Sheds-Patio',
    'Wines-Spirits-Beer',
    'Grocery-Household',
    'Camping-Sports-Spas-Leisure',
    'Home-Improvement',
    'Tyres-Automotive',
    'Toys-Baby-Child',
    'Jewellery-Accessories-Clothing',
    'Health-Wellness',
    'Personal-Care',
)

session = requests.Session()

# ###### Uncomment to run with proxies   ###############################

# proxy = {
#     "http": "http://demoend:test123@35.230.150.209:3128/",
#     "https": "http://demoend:test123@35.230.150.209:3128"
# }
#
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


def save_links_to_csv(url='https://www.costco.co.uk/sitemap_uk_product.xml',selected_categories=selected_categories):
    """Importing links from XML page to CSV-file"""
    xml_page = request(url=url)
    links = BS(xml_page, 'lxml').find_all('loc')
    n = 1
    with open(f'links.csv', 'w') as csv_file:
        spam = csv.writer(csv_file)
        for link in links:
            main_category = link.get_text()[25:].split('/')[0]
            if search_all or main_category in selected_categories:
                spam.writerow([n, main_category, link.get_text()])
                n += 1
    return n


def iter_links():
    """Link iteration from CSV-file"""
    with open('links.csv') as file:
        links = csv.reader(file)
        for link in links:
            main_category = link[1]
            html_page = request(url=link[2])
            yield (html_page, main_category)


def parse_data(page, category):
    """Parse items"""
    soup = BS(page, 'lxml')
    all_json_data = soup.find_all('script', {'type': 'application/ld+json'})
    if len(all_json_data) < 2:
        return None
    json_data = all_json_data[1].get_text()
    data_raw = json.loads(json_data)
    try:
        image_url = soup.find('div', class_="product-image")['data-product-img-url']
    except:
        image_url = None
    try:
        subcategory = soup.find('ol', class_='breadcrumb').find_all('li')[-1].a.get('title')
    except:
        subcategory = None
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
            'subcategory': subcategory,
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
        number_of_links = save_links_to_csv()
        for n, page in enumerate(iter_links()):
            print(f'{n} of {number_of_links}')
            get_data = parse_data(*page)
            if get_data:
                all_objects.append(get_data)
            print(get_data)
    except Exception as ex:
        print(ex)
    finally:
        print(f'saving to output_json/data-{str(datetime.datetime.now())[:-7]}.json')
        with open(f'output_json/data-{str(datetime.datetime.now())[:-7]}.json', 'w', encoding='utf-8') as json_file:
            json.dump(all_objects, json_file, indent=4, separators=(',', ': '), ensure_ascii=False)


if __name__ == '__main__':
    main()
